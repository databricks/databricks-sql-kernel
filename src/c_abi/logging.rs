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

//! `tracing` → C-callback log bridge for C-ABI (ODBC / Go) consumers.
//!
//! The pyo3 and napi bindings forward the kernel's `tracing` output into the
//! host language's logger **in-process** (they link the kernel crate directly,
//! so they can install a `tracing` `Subscriber` / `log` sink in the kernel's
//! address space — see `napi/src/logger.rs`, `pyo3/src/logging.rs`). A C-ABI
//! consumer can't do that: it reaches the kernel only across the C boundary,
//! and its only kernel-log path today is `kernel_init_logging`'s
//! `RUST_LOG` → stderr subscriber. This module adds the missing reverse-call
//! path: a host registers a C callback (`kernel_set_log_callback`) that the
//! kernel invokes with each `databricks*`-targeted event.
//!
//! ## Shape (mirrors the napi bridge)
//!
//! - [`kernel_set_log_callback`] installs a process-global [`KernelLogLayer`]
//!   (once) and records the `(callback, ctx)` sink. The layer captures
//!   `databricks*` events, filters by a per-target threshold (an explicit
//!   one-knob level, else `RUST_LOG`'s per-target `EnvFilter` precedence),
//!   formats each into a [`Record`], and pushes it onto a **bounded** channel
//!   (drop-on-full — logs are advisory and must never block or back-pressure a
//!   kernel hot path).
//! - A dedicated drain thread pulls records and invokes the C callback once per
//!   record with borrowed C strings (valid only for the call).
//!
//! ## Threading & safety contract (documented in `databricks_kernel.h`)
//!
//! The callback fires from the drain thread (never a kernel tokio worker), so
//! the host callback must be thread-safe. It must NOT call back into any
//! `kernel_*` entry point: unlike a call from a tokio worker (which
//! `block_on_ffi` rejects), the drain thread is a plain `std::thread`, so a
//! reentrant blocking `kernel_*` call is NOT caught — it would run its own
//! `block_on`, stalling the drain thread and dropping queued logs. A callback
//! that unwinds is contained: the type is `extern "C-unwind"` and the drain
//! thread wraps every invocation in `catch_unwind`, so a panicking host callback
//! poisons the sink (delivery stops) rather than aborting the process — but a
//! well-behaved callback still must not unwind, since poisoning silently drops
//! all subsequent logs.
//!
//! ## Mutual exclusion with `kernel_init_logging`
//!
//! Both this and `kernel_init_logging` install the one process-global `tracing`
//! subscriber; only the first `set_global_default` wins. If a global subscriber
//! was already installed, `kernel_set_log_callback` returns `Internal` (with a
//! stored last error), exactly as `kernel_init_logging` does for the reverse —
//! a C consumer picks ONE of the two. Last-writer-wins for the sink itself
//! (the callback slot is refreshed on every call).

// The callback type mirrors the C typedef name `kernel_log_callback` (snake
// case) so the Rust and header names read identically, matching the rest of the
// C-ABI surface (`kernel_*_t` handles).
#![allow(non_camel_case_types)]

use super::KernelStatusCode;
use crate::kernel_error::{Error, ErrorCode};
use std::ffi::{c_void, CString};
use std::os::raw::{c_char, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Mutex, OnceLock, RwLock};
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Filter, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// Host log sink: a C function pointer plus an opaque host context passed back
/// verbatim on every call. `level` is 1=ERROR..5=TRACE (mirrors the bridge's
/// numeric severity); `target` and `message` are NUL-terminated C strings valid
/// only for the duration of the call.
///
/// The ABI is `C-unwind`, not plain `C`: a plain `extern "C"` fn that panics
/// aborts at its OWN boundary (Rust ≥ 1.81), so an outer `catch_unwind` around
/// the call could never contain it. `C-unwind` lets a panic propagate across the
/// boundary so `invoke_callback`'s `catch_unwind` can catch a misbehaving host
/// callback and poison the sink instead of aborting the process. The C
/// representation is identical (`void (*)(void*, int, const char*, const char*)`),
/// and non-unwinding C / Go-with-recover callbacks are unaffected.
pub type kernel_log_callback = extern "C-unwind" fn(
    ctx: *mut c_void,
    level: c_int,
    target: *const c_char,
    message: *const c_char,
);

/// A drained log record awaiting delivery to the host callback.
struct Record {
    level: c_int,
    /// `&'static str`: `tracing` metadata targets are static, so this avoids a
    /// per-record heap allocation on chatty DEBUG/TRACE workloads (the message,
    /// built by the field visitor, still owns its `String`).
    target: &'static str,
    message: String,
}

/// The registered sink. `ctx` is stored as a `usize` so the struct is `Send`
/// (the host owns the pointer and guarantees its validity until it detaches);
/// the kernel treats it as opaque and never dereferences it.
struct Sink {
    cb: kernel_log_callback,
    ctx: usize,
    /// Monotonic registration id. The drain thread captures this alongside
    /// `(cb, ctx)` and only poisons the sink on a callback unwind if this id is
    /// still the installed one — so a slow/old callback that unwinds AFTER the
    /// host re-registered a new one cannot kill the replacement.
    gen: u64,
}

const CHANNEL_CAPACITY: usize = 4096;

const LEVEL_OFF: u8 = 0;
const LEVEL_ERROR: u8 = 1;
const LEVEL_WARN: u8 = 2;
const LEVEL_INFO: u8 = 3;
const LEVEL_DEBUG: u8 = 4;
const LEVEL_TRACE: u8 = 5;

/// Default forward threshold for kernel targets not matched by a more specific
/// [`DIRECTIVES`] entry: the explicit one-knob level, `RUST_LOG`'s bare global
/// level, or the static `warn` default. Numeric severity, low = more severe;
/// `0` means "off / emit nothing".
static LEVEL: AtomicU8 = AtomicU8::new(LEVEL_WARN);

/// Most verbose threshold across [`LEVEL`] and every [`DIRECTIVES`] entry, for
/// `max_level_hint` so the process-global gate admits everything any directive
/// could enable (the per-event [`KernelFilter::enabled`] then applies the
/// precise per-target threshold). Tracking only [`LEVEL`] here would wrongly
/// gate out a more-verbose submodule directive before it reached `enabled`.
static MAX_LEVEL: AtomicU8 = AtomicU8::new(LEVEL_WARN);

/// Fast-path flag: whether [`DIRECTIVES`] holds any per-target directive. The
/// common one-knob / bare-level case has none, so the hot path
/// ([`threshold_for_target`]) reads [`LEVEL`] directly and never takes the
/// [`DIRECTIVES`] lock.
static HAS_DIRECTIVES: AtomicBool = AtomicBool::new(false);

/// Per-target `RUST_LOG` thresholds `(target, level)`, applied with `EnvFilter`
/// precedence: a directive matches an event when its target is a **prefix** of
/// the event target (`event_target.starts_with(target)`), and the most specific
/// (longest) match wins. Collapsing these into one scalar would let a verbose
/// submodule directive (`databricks_sql_kernel::reader=trace`) widen the
/// canonical `klog!` target (`databricks::sql::kernel`) that `EnvFilter` would
/// keep at its own directive's level. Consulted only when [`HAS_DIRECTIVES`].
static DIRECTIVES: OnceLock<RwLock<Vec<(String, u8)>>> = OnceLock::new();

/// Count of log records dropped because the bounded channel was full (a slow
/// host callback) or the drain thread stopped. Surfaced via
/// [`kernel_logging_stats`] and via a synthetic "dropped N" line the drain
/// thread emits on recovery, so drop-on-full loss is observable rather than
/// silent (parity with the napi bridge's `DROPPED`).
static DROPPED: AtomicU64 = AtomicU64::new(0);

/// Set if the host callback ever unwound (panicked) across the FFI boundary.
/// The `extern "C-unwind"` type lets the panic propagate so the drain thread's
/// `catch_unwind` catches it (a plain `extern "C"` would instead abort the
/// process at the callback's own boundary); we record it here and stop
/// delivering — a callback that panics once is assumed to keep panicking, so we
/// protect the process rather than re-invoke it. Surfaced via
/// [`kernel_logging_stats`] (`installed` reads `false` while poisoned). Cleared
/// when a NEW callback is registered, so a host can recover by re-installing.
static SINK_POISONED: AtomicBool = AtomicBool::new(false);

const INSTALL_PENDING: u8 = 0;
const INSTALL_OK: u8 = 1;
/// Lost the one-global-subscriber race (e.g. `kernel_init_logging` won) — a
/// benign outcome; the host picks one of the two logging routes.
const INSTALL_ALREADY_SET: u8 = 2;
/// The OS refused to start the drain thread (EAGAIN/ENOMEM) before the global
/// subscriber was claimed — a real failure the host should surface distinctly.
/// NON-terminal: a later `kernel_set_log_callback` retries the install (the
/// subscriber slot was never touched), so a transient spawn failure doesn't
/// permanently disable callback logging.
const INSTALL_SPAWN_FAILED: u8 = 3;

/// Outcome of the subscriber install. `INSTALL_OK` and `INSTALL_ALREADY_SET` are
/// terminal (no further attempt); `INSTALL_PENDING` and `INSTALL_SPAWN_FAILED`
/// are retryable. Anything other than `INSTALL_OK` means kernel logs won't reach
/// the callback; the two failure discriminants let the caller tell the benign
/// already-installed case from a (retryable) spawn failure.
static INSTALL_STATUS: AtomicU8 = AtomicU8::new(INSTALL_PENDING);

/// The drain-thread spawn error string, populated only on `INSTALL_SPAWN_FAILED`
/// so the returned `Internal` can carry the underlying OS error for triage.
static SPAWN_ERROR: OnceLock<Mutex<Option<String>>> = OnceLock::new();

/// Producer end, published once after the drain thread is confirmed running so
/// a live producer never outlives its consumer.
static SENDER: OnceLock<SyncSender<Record>> = OnceLock::new();

/// Current sink. Last-writer-wins; read by the drain thread per record.
static SINK: OnceLock<Mutex<Option<Sink>>> = OnceLock::new();

/// Source of monotonic sink registration ids ([`Sink::gen`]). Bumped under the
/// `SINK` lock on every (re-)registration so the drain thread can tell whether
/// the callback it just invoked is still the installed one.
static NEXT_SINK_GEN: AtomicU64 = AtomicU64::new(0);

/// Serializes install attempts (subscriber + channel + drain-thread spawn).
///
/// Deliberately NOT a `Once`: a `Once` is consumed by the first attempt, so a
/// transient drain-thread spawn failure (EAGAIN/ENOMEM) would permanently
/// disable callback logging for the process — no later `kernel_set_log_callback`
/// could retry. A plain lock + retryable [`INSTALL_STATUS`] lets a spawn failure
/// stay non-terminal so a subsequent call tries again, while `set_global_default`
/// is still attempted at most once (only after a drain thread is confirmed up,
/// which transitions to a terminal state).
static INSTALL_LOCK: Mutex<()> = Mutex::new(());

fn severity(level: &Level) -> u8 {
    match *level {
        Level::ERROR => LEVEL_ERROR,
        Level::WARN => LEVEL_WARN,
        Level::INFO => LEVEL_INFO,
        Level::DEBUG => LEVEL_DEBUG,
        Level::TRACE => LEVEL_TRACE,
    }
}

/// Parse a bare level word into the numeric severity, unknown → `None`.
fn parse_level_word(s: &str) -> Option<u8> {
    match s.trim().to_ascii_lowercase().as_str() {
        "off" | "none" | "silent" => Some(LEVEL_OFF),
        "error" => Some(LEVEL_ERROR),
        "warn" | "warning" => Some(LEVEL_WARN),
        "info" => Some(LEVEL_INFO),
        "debug" => Some(LEVEL_DEBUG),
        "trace" => Some(LEVEL_TRACE),
        _ => None,
    }
}

/// Parse a `RUST_LOG`-style directive string into the kernel-applicable
/// per-target directives plus an optional bare global level, so the callback
/// can filter per target exactly as the stderr path (`kernel_init_logging`)'s
/// `EnvFilter` would — rather than collapsing everything into one scalar.
///
/// Returns `(directives, global)`:
///
///   - `directives`: each retained `(target, level)` keeps its target string so
///     the hot path can apply `EnvFilter`'s prefix rule per event — a directive
///     matches an event only when its target is a **prefix of the event target**
///     (`event_target.starts_with(target)`), and among matches the most specific
///     (longest) wins ([`resolve_threshold`]). Keeping them separate is what
///     stops a verbose submodule directive
///     (`databricks_sql_kernel::reader::inline=trace`) from widening the
///     canonical `klog!` target (`databricks::sql::kernel`), which `EnvFilter`
///     would keep at its own `databricks::sql::kernel=error` directive.
///   - `global`: a bare level (`debug`), the least-specific match `EnvFilter`
///     recognises, used as the default for kernel targets no directive matches.
///
/// Applicability is judged by [`directive_targets_kernel`], NOT a bare
/// `starts_with("databricks")`: a non-matching sibling such as `databricksfoo=warn`
/// or an extension of the fixed-depth `klog!` target
/// (`databricks::sql::kernel::foo=trace`) is a prefix of no kernel event, so
/// `EnvFilter` skips it — and so does this. A submodule directive on the
/// module-path namespace the bridge forwards
/// (`databricks_sql_kernel::session=debug`) IS retained (those events are
/// forwarded by `is_kernel_target`). Foreign directives (`hyper=warn`) are
/// dropped, since the bridge never forwards their events.
fn parse_rust_log(rust_log: &str) -> (Vec<(String, u8)>, Option<u8>) {
    let mut directives: Vec<(String, u8)> = Vec::new();
    let mut global: Option<u8> = None;
    for directive in rust_log.split(',') {
        let d = directive.trim();
        if d.is_empty() {
            continue;
        }
        match d.split_once('=') {
            Some((target, level)) => {
                let target = target.trim();
                if directive_targets_kernel(target) {
                    if let Some(l) = parse_level_word(level) {
                        directives.push((target.to_owned(), l));
                    }
                }
            }
            None => {
                // A bare word is either a global level (`debug`) or a bare target;
                // only treat it as a level if it parses as one.
                if let Some(l) = parse_level_word(d) {
                    global = Some(l);
                }
            }
        }
    }
    (directives, global)
}

/// The crate's module-path root: the `#[instrument]` / default-target namespace
/// (`databricks_sql_kernel::*`) the bridge also forwards (see `is_kernel_target`).
const KERNEL_MODULE_ROOT: &str = "databricks_sql_kernel";

/// The kernel's canonical `tracing` target roots: the `klog!` target
/// ([`KERNEL_LOG_TARGET`](crate::klog::KERNEL_LOG_TARGET),
/// `databricks::sql::kernel`) and the crate's module-path root
/// ([`KERNEL_MODULE_ROOT`]).
const KERNEL_TARGET_ROOTS: [&str; 2] = [crate::klog::KERNEL_LOG_TARGET, KERNEL_MODULE_ROOT];

/// Whether a `RUST_LOG` directive target selects kernel events under
/// `EnvFilter`'s `event_target.starts_with(directive_target)` prefix match,
/// against the targets the bridge actually forwards. A directive applies when:
///
///   - its target is a **prefix of a canonical root** (`databricks`,
///     `databricks::sql`, `databricks_sql_kernel`, …) — it matches events at
///     that root; or
///   - its target **extends the module-path root** (`databricks_sql_kernel::session`)
///     — those submodule events are forwarded by `is_kernel_target`, so a host
///     targeting a specific kernel submodule must raise the threshold rather
///     than be silently ignored.
///
/// It is rejected otherwise: an unrelated `databricks`-prefixed sibling
/// (`databricksfoo`) matches no kernel event, and an extension of the
/// fixed-depth `klog!` target (`databricks::sql::kernel::foo`) matches nothing
/// either, since the kernel never emits deeper than `databricks::sql::kernel`.
fn directive_targets_kernel(target: &str) -> bool {
    target.starts_with(KERNEL_MODULE_ROOT)
        || KERNEL_TARGET_ROOTS
            .iter()
            .any(|root| root.starts_with(target))
}

/// Resolve the forward threshold for `target` under `EnvFilter` precedence: the
/// most specific (longest) directive whose target is a **prefix** of `target`
/// wins; on a specificity tie the later entry wins (matching `EnvFilter`'s
/// last-writer dedup); with no matching directive the `default` applies. Pure
/// (no globals) so the precedence logic is unit-testable without touching the
/// process-global filter state.
fn resolve_threshold(target: &str, default: u8, directives: &[(String, u8)]) -> u8 {
    let mut best: Option<(usize, u8)> = None;
    for (t, lvl) in directives {
        if target.starts_with(t.as_str()) {
            let specificity = t.len();
            let wins = best.is_none_or(|(len, _)| specificity >= len);
            if wins {
                best = Some((specificity, *lvl));
            }
        }
    }
    best.map_or(default, |(_, l)| l)
}

/// The current forward threshold for `target`, reading the process-global filter
/// state. Fast path: with no per-target directives (the one-knob / bare-level
/// case) this is a single relaxed load of [`LEVEL`] and never locks.
fn threshold_for_target(target: &str) -> u8 {
    let default = LEVEL.load(Ordering::Relaxed);
    if !HAS_DIRECTIVES.load(Ordering::Relaxed) {
        return default;
    }
    match DIRECTIVES.get().and_then(|lock| lock.read().ok()) {
        Some(dirs) => resolve_threshold(target, default, &dirs),
        None => default,
    }
}

/// Returns whether an event at `level` on `target` should currently be
/// forwarded, applying the per-target threshold ([`threshold_for_target`]).
fn should_emit(target: &str, level: &Level) -> bool {
    let threshold = threshold_for_target(target);
    threshold != LEVEL_OFF && severity(level) <= threshold
}

/// Targets the bridge forwards: the `klog!` shim's `databricks::sql::kernel`
/// and kernel module paths (`databricks_sql_kernel::*`). A raw `starts_with`
/// (not a `::`-segmented filter) so the underscore module-path variants match.
fn is_kernel_target(target: &str) -> bool {
    target.starts_with("databricks")
}

/// The most-verbose configured level as a `tracing` [`LevelFilter`], for
/// `max_level_hint` so the process-global gate tracks the kernel level instead
/// of pinning at TRACE. Reads [`MAX_LEVEL`] (the max across the default and all
/// per-target directives) so a verbose submodule directive isn't gated out
/// before the per-event [`KernelFilter::enabled`] can apply it.
fn current_level_filter() -> LevelFilter {
    match MAX_LEVEL.load(Ordering::Relaxed) {
        LEVEL_OFF => LevelFilter::OFF,
        LEVEL_ERROR => LevelFilter::ERROR,
        LEVEL_WARN => LevelFilter::WARN,
        LEVEL_INFO => LevelFilter::INFO,
        LEVEL_DEBUG => LevelFilter::DEBUG,
        _ => LevelFilter::TRACE,
    }
}

/// Per-callsite interest. Kernel EVENT callsites re-evaluate per event
/// (`sometimes`, for the dynamic level). Everything else caches off forever
/// (`never`):
///
///   - foreign targets (`hyper`/`rustls`/…) never dispatch, and
///   - SPAN callsites never dispatch — even on kernel targets. This bridge is
///     event-only ([`KernelLogLayer`] implements just `on_event` and never
///     reads span data), so admitting spans would make the kernel's own
///     `databricks*` spans (`Session`'s `info_span!`, the `#[instrument]` on
///     the `next_batch()` hot path) pay span create/enter/exit in the registry
///     at INFO/DEBUG/TRACE thresholds for output that never comes. Caching them
///     off turns those into no-op disabled spans.
fn callsite_interest_for(target: &str, is_event: bool) -> Interest {
    if is_event && is_kernel_target(target) {
        Interest::sometimes()
    } else {
        Interest::never()
    }
}

/// Collects a `tracing` event's `message` plus structured fields into one line:
/// `"<message> key1=v1 key2=v2"`.
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

/// Process-global `tracing` Layer: formats each kernel event and enqueues it
/// (drop-on-full). The per-layer [`KernelFilter`] already excludes foreign
/// targets and below-level events.
struct KernelLogLayer;

/// Per-layer filter gating the bridge: foreign callsites cache off, kernel
/// callsites re-evaluate per event, and `max_level_hint` tracks the level.
struct KernelFilter;

impl<S> Filter<S> for KernelFilter {
    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> Interest {
        callsite_interest_for(meta.target(), meta.is_event())
    }

    fn enabled(&self, meta: &Metadata<'_>, _ctx: &Context<'_, S>) -> bool {
        // Event-only: never enable spans (the layer has no span consumer). The
        // per-target threshold applies `RUST_LOG`'s `EnvFilter` precedence so a
        // verbose submodule directive can't widen the canonical `klog!` target.
        meta.is_event()
            && is_kernel_target(meta.target())
            && should_emit(meta.target(), meta.level())
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
        let Some(tx) = SENDER.get() else {
            return;
        };
        // Once a callback has unwound the sink is poisoned and the drain thread
        // drops everything until re-registration. Short-circuit HERE — before the
        // field visit, the message allocation, and the enqueue — so a poisoned
        // sink stops paying producer-side formatting for records it will only
        // drop. Still count the loss so kernel_logging_stats stays accurate
        // (parity with the drain thread's own poison drop-count; each record is
        // counted exactly once — here if produced while poisoned, or by the drain
        // thread if it was already queued when the poison landed).
        if SINK_POISONED.load(Ordering::Relaxed) {
            DROPPED.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let meta = event.metadata();
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let record = Record {
            level: severity(meta.level()) as c_int,
            target: meta.target(),
            message: visitor.finish(),
        };
        // Drop-on-full / disconnected: logs are advisory, never block a kernel
        // task on a slow host callback. Count the loss so it's observable (via
        // kernel_logging_stats and the drain thread's synthetic recovery line)
        // rather than vanishing silently.
        if tx.try_send(record).is_err() {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Invoke the host callback once, guarded against an unwind. A panic crossing
/// `extern "C"` aborts the process (Rust ≥ 1.81), so a host callback that
/// unwinds must be contained here — this is the same `catch_unwind` guard every
/// other FFI boundary in the crate uses (`block_on_ffi`, `catch_ffi`). Returns
/// `false` if the callback unwound, so the caller can poison the sink.
fn invoke_callback(
    cb: kernel_log_callback,
    ctx: usize,
    level: c_int,
    target: &str,
    message: &str,
) -> bool {
    // Interior NULs can't appear in a C string; on the rare event whose text
    // contains one, TRUNCATE at the first NUL rather than replacing the whole
    // string with a placeholder — the informative prefix (up to the NUL) is
    // preserved. CString::new only fails on an interior NUL, so on error rebuild
    // from the bytes up to it (which cannot themselves contain a NUL → unwrap is
    // infallible).
    let to_cstring = |s: &str| {
        CString::new(s).unwrap_or_else(|e| {
            let n = e.nul_position();
            CString::new(&s.as_bytes()[..n]).unwrap()
        })
    };
    let target = to_cstring(target);
    let message = to_cstring(message);
    catch_unwind(AssertUnwindSafe(|| {
        cb(ctx as *mut c_void, level, target.as_ptr(), message.as_ptr());
    }))
    .is_ok()
}

/// Poison the sink — stop delivery — but ONLY if generation `gen` is still the
/// installed registration. The drain thread copies `(cb, ctx, gen)` out and
/// runs the callback without the lock, so the host may re-register (bumping the
/// generation and clearing the poison) while an OLD callback is mid-unwind.
/// Comparing under the `SINK` lock (which the setter also holds while it swaps
/// the sink + clears the poison) means an old callback's failure can never kill
/// the newer registration.
fn poison_sink_if_current(gen: u64) {
    if let Some(lock) = SINK.get() {
        if let Ok(guard) = lock.lock() {
            if guard.as_ref().map(|s| s.gen) == Some(gen) {
                SINK_POISONED.store(true, Ordering::Relaxed);
            }
        }
    }
}

/// Drain loop (own OS thread): pull one record at a time and invoke the host
/// callback with borrowed C strings. Runs off the kernel's tokio workers so a
/// slow callback can't stall query execution.
fn drain_loop(rx: Receiver<Record>) {
    // Drop count already reported to the host, so we only emit a synthetic
    // "dropped N" notice for the delta since the last delivered record.
    let mut reported_drops: u64 = 0;
    // `recv` errors only once the (process-global) sender is dropped, which
    // happens on a lost subscriber race (the setter drops the sender so this
    // thread exits) or process teardown; exit cleanly either way.
    while let Ok(record) = rx.recv() {
        // A callback that unwound once is assumed to keep unwinding; stop
        // delivering to protect the process (the loss is counted + surfaced via
        // kernel_logging_stats). Keep draining so the channel doesn't wedge.
        if SINK_POISONED.load(Ordering::Relaxed) {
            DROPPED.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let Some(sink_lock) = SINK.get() else {
            continue;
        };
        // Copy the sink out under the lock (cheap: a fn pointer + usize + gen)
        // so the callback runs without holding the mutex — a reentrant setter
        // can't deadlock, and a slow callback can't block a level/sink update.
        let sink = match sink_lock.lock() {
            Ok(g) => g.as_ref().map(|s| (s.cb, s.ctx, s.gen)),
            Err(_) => None,
        };
        let Some((cb, ctx, gen)) = sink else {
            continue;
        };
        // Surface any records dropped since the last delivery as one synthetic
        // WARN line through the callback itself, so a host that only reads the
        // callback stream still learns it missed events (kernel_logging_stats
        // carries the cumulative count for a host that polls).
        let dropped = DROPPED.load(Ordering::Relaxed);
        if dropped > reported_drops {
            let notice = format!(
                "{} kernel log record(s) dropped (slow log callback); increase callback throughput",
                dropped - reported_drops
            );
            reported_drops = dropped;
            if !invoke_callback(
                cb,
                ctx,
                LEVEL_WARN as c_int,
                is_kernel_target_str(),
                &notice,
            ) {
                // The notice unwound, so `record` won't be delivered either —
                // count it as dropped (the health contract), then poison iff
                // this callback is still the installed one.
                DROPPED.fetch_add(1, Ordering::Relaxed);
                poison_sink_if_current(gen);
                continue;
            }
        }
        if !invoke_callback(cb, ctx, record.level, record.target, &record.message) {
            // The callback unwound and did NOT deliver this record — count the
            // loss so kernel_logging_stats reflects it (previously the first
            // failing record was silently uncounted), then poison iff this
            // callback is still the installed one.
            DROPPED.fetch_add(1, Ordering::Relaxed);
            poison_sink_if_current(gen);
        }
    }
}

/// The kernel log target as a plain `&str`, for the synthetic drop-notice line
/// so it carries the same target as real kernel events.
fn is_kernel_target_str() -> &'static str {
    crate::klog::KERNEL_LOG_TARGET
}

/// A resolved forward filter: the default threshold for unmatched kernel
/// targets plus the per-target `RUST_LOG` directives ([`resolve_threshold`]).
struct ResolvedFilter {
    /// Threshold for kernel targets no `directives` entry matches — the explicit
    /// one-knob level, `RUST_LOG`'s bare global level, or the `warn` default.
    default_level: u8,
    /// Per-target `RUST_LOG` thresholds. Empty for the one-knob explicit-level
    /// case (a uniform threshold), so the hot path stays lock-free.
    directives: Vec<(String, u8)>,
}

/// Resolve the callback's forward filter from the caller's explicit `level`
/// argument, mirroring `kernel_init_logging`'s precedence so the two C-ABI
/// logging routes filter identically:
///
///   1. an explicit, recognised `level` word wins as a UNIFORM one-knob
///      threshold (e.g. Go's driver level mapped through `DATABRICKS_LOG_LEVEL`),
///      with no per-target directives — the host asked for one level everywhere;
///   2. otherwise `RUST_LOG`, parsed into per-target directives + a bare global
///      level ([`parse_rust_log`]), applied with `EnvFilter` precedence;
///   3. otherwise `None` — the current filter is left untouched.
///
/// This keeps the callback on the one-knob contract PECOBLR-3650 established:
/// `kernel_init_logging` maps the driver level to a level string, and the
/// callback path must honour that same string rather than only `RUST_LOG` —
/// otherwise a host at `DATABRICKS_LOG_LEVEL=debug` (but no `RUST_LOG`) would
/// silently drop kernel debug lines.
///
/// Note the target matching still differs slightly from `kernel_init_logging`:
/// that path builds an `EnvFilter` (`::`-segment match), whereas this bridge
/// matches the broader `databricks` prefix (see `is_kernel_target`) for
/// forwarding, then applies the same per-target precedence for the threshold. So
/// for a `RUST_LOG` target directive the two routes select the same threshold
/// but not necessarily the identical event set.
fn resolve_filter(explicit: Option<&str>) -> Option<ResolvedFilter> {
    if let Some(word) = explicit {
        // A recognised explicit level always wins as a uniform threshold. "off"
        // is honoured (the host asked for silence); an unrecognised word falls
        // through to RUST_LOG.
        if let Some(l) = parse_level_word(word) {
            return Some(ResolvedFilter {
                default_level: l,
                directives: Vec::new(),
            });
        }
    }
    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        let (directives, global) = parse_rust_log(&rust_log);
        if !directives.is_empty() || global.is_some() {
            return Some(ResolvedFilter {
                // A `RUST_LOG` with only target directives (no bare level) leaves
                // unmatched kernel targets at the static `warn` default.
                default_level: global.unwrap_or(LEVEL_WARN),
                directives,
            });
        }
    }
    None
}

/// Install `filter` into the process-global filter state, returning whether it
/// actually changed anything (so the caller only rebuilds the callsite interest
/// cache when it matters). Writes the directives first, then the fast-path flag,
/// then the levels, so a concurrent reader never sees `HAS_DIRECTIVES` set
/// against a stale directive list.
fn apply_filter(filter: ResolvedFilter) -> bool {
    let new_default = filter.default_level;
    let new_max = filter
        .directives
        .iter()
        .map(|(_, l)| *l)
        .fold(new_default, u8::max);
    let has_dirs = !filter.directives.is_empty();

    let dirs_lock = DIRECTIVES.get_or_init(|| RwLock::new(Vec::new()));
    let dirs_changed = match dirs_lock.write() {
        Ok(mut w) => {
            let changed = *w != filter.directives;
            *w = filter.directives;
            changed
        }
        // Couldn't inspect the old list (poisoned lock) — assume changed so the
        // interest cache is rebuilt conservatively.
        Err(_) => true,
    };
    HAS_DIRECTIVES.store(has_dirs, Ordering::Relaxed);
    MAX_LEVEL.store(new_max, Ordering::Relaxed);
    let prev_default = LEVEL.swap(new_default, Ordering::Relaxed);
    prev_default != new_default || dirs_changed
}

/// Stand up the process-global subscriber + drain thread, retryable across calls
/// (see [`INSTALL_LOCK`]). Returns `true` iff THIS call performed the successful
/// install (so the caller rebuilds the interest cache for the freshly-installed
/// filter). A benign lost-race or a spawn failure returns `false`; the outcome
/// is recorded in [`INSTALL_STATUS`] for the caller's return code.
///
/// Ordering matters: stand up the drain thread FIRST, and only then claim the
/// one global subscriber via `set_global_default`. This keeps two failure modes
/// clean:
///
///   - Spawn failure: if the OS refuses the drain thread (EAGAIN/ENOMEM) we bail
///     out BEFORE touching the global subscriber, so the slot stays free and the
///     host can still fall back to `kernel_init_logging`. The status stays
///     retryable, so a later call can try again. (Claiming the subscriber first
///     would strand it with no `SENDER`, silently dropping every event.)
///   - Lost race: if another subscriber (e.g. `kernel_init_logging`) already
///     won, we DROP the sender so the freshly-spawned drain thread's `recv()`
///     errors and it exits — no idle-thread leak, and `SENDER` stays unset so
///     `on_event` short-circuits.
///
/// `set_global_default` is attempted at most once per process: it runs only
/// after a drain thread is confirmed up, and both its outcomes (won / lost) are
/// terminal, so a retry never re-attempts it.
fn ensure_installed() -> bool {
    // Terminal states need no work and no lock.
    match INSTALL_STATUS.load(Ordering::Acquire) {
        INSTALL_OK | INSTALL_ALREADY_SET => return false,
        _ => {}
    }
    let Ok(_guard) = INSTALL_LOCK.lock() else {
        return false;
    };
    // Re-check under the lock: another caller may have installed while we waited.
    match INSTALL_STATUS.load(Ordering::Acquire) {
        INSTALL_OK | INSTALL_ALREADY_SET => return false,
        _ => {}
    }
    // Stand up the delivery path first (thread + channel), so a spawn failure
    // never leaves the global subscriber slot occupied.
    let (tx, rx) = sync_channel::<Record>(CHANNEL_CAPACITY);
    if let Err(e) = std::thread::Builder::new()
        .name("kernel-log-drain".to_owned())
        .spawn(move || drain_loop(rx))
    {
        // Record the OS error distinctly so on-call can tell a spawn failure from
        // the benign "already installed" case. Use `.lock().ok()` (not
        // `.unwrap()`) so a poisoned mutex can't panic across the FFI boundary.
        // `tx` drops here (no thread was spawned); the global subscriber is left
        // untouched, and the status stays retryable for a later call.
        if let Ok(mut guard) = SPAWN_ERROR.get_or_init(|| Mutex::new(None)).lock() {
            *guard = Some(e.to_string());
        }
        INSTALL_STATUS.store(INSTALL_SPAWN_FAILED, Ordering::Release);
        return false;
    }
    // Drain thread is running. Now claim the one global subscriber.
    let subscriber = tracing_subscriber::registry().with(KernelLogLayer.with_filter(KernelFilter));
    if tracing::subscriber::set_global_default(subscriber).is_err() {
        // Another global subscriber won. Drop the sender so the drain thread
        // (blocked on recv()) unblocks with an error and exits cleanly, then
        // leave SENDER unset so on_event's `let Some(tx)` short-circuits.
        drop(tx);
        INSTALL_STATUS.store(INSTALL_ALREADY_SET, Ordering::Release);
        return false;
    }
    // We own the subscriber AND have a live consumer. Publish SENDER only now, so
    // a live producer never outlives its consumer.
    let _ = SENDER.set(tx);
    INSTALL_STATUS.store(INSTALL_OK, Ordering::Release);
    true
}

/// Install (process-wide) a callback that receives kernel `databricks*`
/// `tracing` events. `ctx` is passed back verbatim on every call. Passing a
/// null `cb` is rejected with `InvalidArgument` (to detach a sink, the design
/// would grow an explicit detach — v0 keeps install-only).
///
/// `level` (a NUL-terminated `OFF`/`ERROR`/`WARN`/`INFO`/`DEBUG`/`TRACE`
/// string, case-insensitive, or NULL) sets the forward filter with the same
/// precedence as `kernel_init_logging`: an explicit recognised level wins as a
/// UNIFORM threshold, else `RUST_LOG` is applied per target with `EnvFilter`
/// precedence (so a submodule directive doesn't widen the canonical `klog!`
/// target). A host that maps its own logger level (Go's `DATABRICKS_LOG_LEVEL`)
/// passes it here so the callback filters on the one knob rather than only
/// `RUST_LOG`. When neither resolves (NULL / unrecognised level AND no matching
/// `RUST_LOG`) the current filter is LEFT UNCHANGED — the static `warn` default
/// on the first-ever call, but the previously-set filter on a re-registration
/// (the filter is process-global and last-writer-wins only when a writer
/// actually resolves one).
///
/// Returns `Internal` (with a stored last error) if a global `tracing`
/// subscriber was already installed (e.g. by `kernel_init_logging`) — the two
/// are mutually exclusive; a C consumer installs exactly one — or if the
/// log-drain thread could not be spawned (a transient spawn failure is
/// retryable: a later call attempts the install again).
///
/// # Safety
/// `cb` must be a valid function pointer that is thread-safe, does not unwind,
/// and does not call back into any `kernel_*` entry point. `ctx` must remain
/// valid until the process exits (there is no detach in v0). `level`, if
/// non-null, must be a NUL-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn kernel_set_log_callback(
    cb: Option<kernel_log_callback>,
    ctx: *mut c_void,
    level: *const c_char,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    let Some(cb) = cb else {
        crate::c_abi::error::store_error(
            Error::invalid_argument().message("kernel_set_log_callback: cb must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    };

    // Resolve the forward threshold: explicit `level` (the host's mapped logger
    // level) wins, else RUST_LOG, else the WARN default is left in place. A
    // non-UTF-8 `level` is treated as absent (best-effort, like
    // kernel_init_logging's level arg) rather than a hard error. This keeps the
    // callback filtering on the same knob as the stderr path.
    let explicit_level = if level.is_null() {
        None
    } else {
        std::ffi::CStr::from_ptr(level).to_str().ok()
    };
    // Track whether the filter actually changed so we only rebuild the (process-
    // wide, lock-taking) callsite interest cache when it matters — a same-filter
    // ctx refresh or a call that loses the subscriber race shouldn't serialize
    // all logging dispatch.
    let mut filter_changed = false;
    if let Some(resolved) = resolve_filter(explicit_level) {
        filter_changed = apply_filter(resolved);
    }

    // Record / refresh the sink (last writer wins). Clear any prior poison: a
    // callback that unwound poisons the sink (drain_loop stops delivering to
    // protect the process), but registering a NEW callback is an explicit
    // request for a fresh start, so honour the last-writer-wins contract and let
    // delivery resume. The drain thread reads the flag per record, so clearing
    // it here (before releasing the SINK lock) is immediately effective.
    let sink = SINK.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = sink.lock() {
        // A fresh generation so the drain thread won't let an old in-flight
        // callback's unwind poison this new registration (see poison_sink_if_current).
        let gen = NEXT_SINK_GEN.fetch_add(1, Ordering::Relaxed);
        *guard = Some(Sink {
            cb,
            ctx: ctx as usize,
            gen,
        });
        SINK_POISONED.store(false, Ordering::Relaxed);
    }

    // Install the process-global subscriber + drain thread (retryable across
    // calls after a transient spawn failure — see ensure_installed).
    let did_install = ensure_installed();
    // Rebuild the process-wide callsite interest cache only when it can matter:
    // on the install that set our filter, or when a later call changed the
    // filter. set_global_default rebuilds on its own, but the filter may have
    // been set before install, and a re-filter after install needs a rebuild
    // too; a same-filter ctx refresh or a race-loss does not (the rebuild takes
    // the global callsite lock and re-evaluates every callsite in the binary).
    if did_install || filter_changed {
        tracing::callsite::rebuild_interest_cache();
    }

    match INSTALL_STATUS.load(Ordering::Acquire) {
        INSTALL_OK => KernelStatusCode::Success,
        INSTALL_SPAWN_FAILED => {
            let detail = SPAWN_ERROR
                .get()
                .and_then(|m| m.lock().ok().and_then(|g| g.clone()))
                .unwrap_or_else(|| "unknown".to_owned());
            crate::c_abi::error::store_and_code(Error::new(
                ErrorCode::Internal,
                format!(
                    "kernel_set_log_callback: the log-drain thread could not be started ({detail}); \
                     kernel logs will not reach the callback"
                ),
            ))
        }
        // INSTALL_ALREADY_SET (or a still-PENDING install we couldn't take the
        // lock for): a global subscriber was already installed, or the install
        // couldn't run. Either way kernel logs won't reach this callback.
        _ => crate::c_abi::error::store_and_code(Error::new(
            ErrorCode::Internal,
            "kernel_set_log_callback: a global tracing subscriber was already installed \
             (e.g. via kernel_init_logging); kernel logs will not reach the callback. \
             Install exactly one of kernel_set_log_callback / kernel_init_logging.",
        )),
    }
}

/// Report the log-callback bridge's health so a host can tell whether kernel
/// logs are actually flowing. Writes to each (optional) out-param:
///
///   - `installed`: `true` iff kernel logs are CURRENTLY being delivered — this
///     callback won the process-global subscriber AND the sink is not poisoned.
///     A callback that unwound poisons the sink (delivery stops); this then
///     reads `false` even though the subscriber is still installed, so a host
///     polling it sees a dead sink rather than a silent no-op. Registering a new
///     callback clears the poison and flips this back to `true`.
///   - `dropped`: cumulative count of kernel log records dropped after the
///     subscriber was installed — because the bounded delivery channel was full
///     (a slow callback) or the sink was poisoned by a callback that unwound.
///     This does NOT cover the drain-thread-never-started case (a spawn failure
///     leaves no channel to overflow); there `installed` reads `false` and
///     `kernel_set_log_callback` returned `Internal` with the spawn error.
///
/// A NULL out-param is skipped. Never fails — a host can poll it to distinguish
/// "logs are quiet" from "logs are being dropped / the sink died / never
/// installed" (parity with the napi binding's `kernelLoggingStats()`).
///
/// Clears the thread-local last-error on entry like every other C-ABI entry
/// point, so a host that calls this (successful, infallible) accessor between a
/// failed API call and reading the error doesn't observe the prior call's stale
/// error through it.
///
/// # Safety
/// Each non-null out-param must point to a writable `bool` / `uint64_t`.
#[no_mangle]
pub unsafe extern "C" fn kernel_logging_stats(installed: *mut bool, dropped: *mut u64) {
    crate::c_abi::error::clear_error();
    if !installed.is_null() {
        // "Currently delivering": won the subscriber AND not poisoned. A poisoned
        // sink is installed-but-dead, which must not read as healthy.
        *installed = INSTALL_STATUS.load(Ordering::Acquire) == INSTALL_OK
            && !SINK_POISONED.load(Ordering::Relaxed);
    }
    if !dropped.is_null() {
        *dropped = DROPPED.load(Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A well-behaved callback returns true (delivered); a callback that unwinds
    // is contained by catch_unwind and returns false (so the drain loop poisons
    // the sink) rather than aborting the process at the extern "C" boundary.
    extern "C-unwind" fn ok_cb(
        _ctx: *mut c_void,
        _lvl: c_int,
        _t: *const c_char,
        _m: *const c_char,
    ) {
    }
    extern "C-unwind" fn panic_cb(
        _ctx: *mut c_void,
        _lvl: c_int,
        _t: *const c_char,
        _m: *const c_char,
    ) {
        panic!("callback boom");
    }

    #[test]
    fn invoke_callback_delivers_and_contains_unwind() {
        assert!(
            invoke_callback(
                ok_cb,
                0,
                LEVEL_WARN as c_int,
                "databricks::sql::kernel",
                "ok"
            ),
            "a normal callback should report delivered"
        );
        assert!(
            !invoke_callback(
                panic_cb,
                0,
                LEVEL_ERROR as c_int,
                "databricks::sql::kernel",
                "boom"
            ),
            "an unwinding callback must be caught and report not-delivered, not abort"
        );
    }

    // A message containing an interior NUL is truncated at the NUL (the prefix is
    // preserved), not replaced wholesale with a placeholder. We capture what the
    // callback receives via a thread-local so the test needs no global state.
    thread_local! {
        static LAST_MSG: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
    }
    extern "C-unwind" fn capture_msg_cb(
        _ctx: *mut c_void,
        _lvl: c_int,
        _t: *const c_char,
        m: *const c_char,
    ) {
        let s = unsafe { std::ffi::CStr::from_ptr(m) }
            .to_string_lossy()
            .into_owned();
        LAST_MSG.with(|c| *c.borrow_mut() = s);
    }

    #[test]
    fn invoke_callback_truncates_interior_nul_keeps_prefix() {
        assert!(invoke_callback(
            capture_msg_cb,
            0,
            LEVEL_WARN as c_int,
            "databricks::sql::kernel",
            "keep-this\0drop-this",
        ));
        LAST_MSG.with(|c| {
            assert_eq!(
                *c.borrow(),
                "keep-this",
                "interior NUL should truncate, preserving the prefix"
            )
        });
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
        let target = crate::klog::KERNEL_LOG_TARGET;
        LEVEL.store(LEVEL_INFO, Ordering::Relaxed);
        assert!(should_emit(target, &Level::ERROR));
        assert!(should_emit(target, &Level::INFO));
        assert!(!should_emit(target, &Level::DEBUG));
        LEVEL.store(LEVEL_OFF, Ordering::Relaxed);
        assert!(!should_emit(target, &Level::ERROR));
        LEVEL.store(LEVEL_WARN, Ordering::Relaxed); // restore
    }

    #[test]
    fn parse_rust_log_keeps_kernel_directives_and_global() {
        // Kernel directives are retained with their target (for per-event prefix
        // matching); foreign directives are dropped; a bare word is the global.
        let (dirs, global) = parse_rust_log("hyper=off,databricks::sql::kernel=info,rustls=warn");
        assert_eq!(
            dirs,
            vec![("databricks::sql::kernel".to_owned(), LEVEL_INFO)]
        );
        assert_eq!(global, None);

        // Either kernel spelling is kept.
        let (dirs, _) = parse_rust_log("databricks_sql_kernel=trace");
        assert_eq!(
            dirs,
            vec![("databricks_sql_kernel".to_owned(), LEVEL_TRACE)]
        );

        // A bare global level with no kernel directive.
        let (dirs, global) = parse_rust_log("debug");
        assert!(dirs.is_empty());
        assert_eq!(global, Some(LEVEL_DEBUG));

        // Nothing kernel-applicable and no bare level → empty.
        let (dirs, global) = parse_rust_log("hyper=warn");
        assert!(dirs.is_empty());
        assert_eq!(global, None);
        let (dirs, global) = parse_rust_log("");
        assert!(dirs.is_empty());
        assert_eq!(global, None);

        // Non-matching databricks siblings and over-specific extensions of the
        // fixed-depth klog target are dropped (prefixes of no kernel event).
        let (dirs, _) = parse_rust_log("databricksfoo=warn,databricks::sql::kernel::foo=trace");
        assert!(dirs.is_empty());
    }

    #[test]
    fn resolve_threshold_per_target_does_not_widen_klog() {
        // The reviewer's case: a klog directive and an unrelated submodule
        // directive must NOT widen each other. Each target resolves against only
        // the directives whose target is a PREFIX of it (EnvFilter's rule), so a
        // single scalar can't collapse them into the most verbose one.
        let (dirs, global) = parse_rust_log(
            "databricks::sql::kernel=error,databricks_sql_kernel::reader::inline=trace",
        );
        assert_eq!(global, None);
        let default = global.unwrap_or(LEVEL_WARN);
        // The canonical klog target keeps ERROR — the reader::inline=trace
        // directive is not a prefix of it, so it does not apply (previously the
        // single scalar collapsed to TRACE and widened klog to trace/debug).
        assert_eq!(
            resolve_threshold("databricks::sql::kernel", default, &dirs),
            LEVEL_ERROR
        );
        // A module event under the submodule keeps its own TRACE directive.
        assert_eq!(
            resolve_threshold(
                "databricks_sql_kernel::reader::inline::stream",
                default,
                &dirs
            ),
            LEVEL_TRACE
        );
        // A kernel target matched by neither directive falls back to the default.
        assert_eq!(
            resolve_threshold("databricks_sql_kernel::session", default, &dirs),
            LEVEL_WARN
        );
    }

    #[test]
    fn resolve_threshold_most_specific_wins_order_independent() {
        // Longest matching prefix wins, independent of order (EnvFilter dedup
        // is last-writer on a tie, so we replace on `>=`).
        let dirs = vec![
            ("databricks".to_owned(), LEVEL_WARN),
            ("databricks::sql::kernel".to_owned(), LEVEL_DEBUG),
        ];
        assert_eq!(
            resolve_threshold("databricks::sql::kernel", LEVEL_WARN, &dirs),
            LEVEL_DEBUG
        );
        let dirs_rev = vec![
            ("databricks::sql::kernel".to_owned(), LEVEL_DEBUG),
            ("databricks".to_owned(), LEVEL_WARN),
        ];
        assert_eq!(
            resolve_threshold("databricks::sql::kernel", LEVEL_WARN, &dirs_rev),
            LEVEL_DEBUG
        );
        // A databricks event matched only by the broad directive takes its level,
        // not the more specific sibling's.
        assert_eq!(
            resolve_threshold("databricks::other", LEVEL_INFO, &dirs),
            LEVEL_WARN
        );
        // No matching directive → the default.
        assert_eq!(resolve_threshold("databricks", LEVEL_INFO, &[]), LEVEL_INFO);
    }

    #[test]
    fn directive_targets_kernel_matches_prefixes_of_kernel_roots() {
        // Prefixes of the klog target and the module-path root are accepted.
        assert!(directive_targets_kernel("databricks"));
        assert!(directive_targets_kernel("databricks::sql"));
        assert!(directive_targets_kernel(crate::klog::KERNEL_LOG_TARGET));
        assert!(directive_targets_kernel("databricks_sql_kernel"));
        // Submodules of the module-path root are accepted (those events are
        // forwarded by is_kernel_target, so a host targeting them must gate).
        assert!(directive_targets_kernel("databricks_sql_kernel::session"));
        assert!(directive_targets_kernel(
            "databricks_sql_kernel::reader::inline"
        ));
        // Unrelated siblings and over-specific extensions of the fixed-depth
        // klog target are rejected.
        assert!(!directive_targets_kernel("databricksfoo"));
        assert!(!directive_targets_kernel("databricks::sql::kernel::foo"));
        assert!(!directive_targets_kernel("hyper"));
    }

    #[test]
    fn resolve_filter_explicit_level_is_uniform_one_knob() {
        // An explicit, recognised level wins outright — this is the one-knob
        // path (Go maps DATABRICKS_LOG_LEVEL and passes it in), independent of
        // whatever RUST_LOG happens to be, and carries NO per-target directives
        // (one level applies everywhere). We don't touch the process RUST_LOG
        // here (that would race other tests); an explicit level short-circuits
        // before RUST_LOG is ever read.
        let f = resolve_filter(Some("debug")).expect("recognised level resolves");
        assert_eq!(f.default_level, LEVEL_DEBUG);
        assert!(
            f.directives.is_empty(),
            "a one-knob explicit level carries no per-target directives"
        );
        assert_eq!(
            resolve_filter(Some("OFF")).unwrap().default_level,
            LEVEL_OFF
        );
        assert_eq!(
            resolve_filter(Some("Error")).unwrap().default_level,
            LEVEL_ERROR
        );
        // An unrecognised explicit word falls through (to RUST_LOG, else None).
        assert_eq!(parse_level_word("not-a-level"), None);
        // NULL/absent explicit level defers to RUST_LOG (covered by the
        // parse_rust_log / resolve_threshold tests).
    }

    #[test]
    fn foreign_callsites_cache_off_kernel_callsites_reevaluate() {
        // Kernel EVENT callsites re-evaluate; foreign ones cache off.
        assert!(callsite_interest_for("hyper::client::pool", true).is_never());
        assert!(callsite_interest_for("rustls::conn", true).is_never());
        assert!(callsite_interest_for("databricks::sql::kernel", true).is_sometimes());
        assert!(callsite_interest_for("databricks_sql_kernel::session", true).is_sometimes());
    }

    #[test]
    fn poison_sink_if_current_respects_generation() {
        // Guards the re-registration race: the drain thread copies a sink's
        // generation out, runs the callback unlocked, and only poisons on unwind
        // if that generation is STILL installed — so an old in-flight callback's
        // failure can't kill a callback the host registered in the meantime.
        let lock = SINK.get_or_init(|| Mutex::new(None));

        // Install a sink at generation `g_old`, unpoisoned.
        let g_old = NEXT_SINK_GEN.fetch_add(1, Ordering::Relaxed);
        *lock.lock().unwrap() = Some(Sink {
            cb: ok_cb,
            ctx: 0,
            gen: g_old,
        });
        SINK_POISONED.store(false, Ordering::Relaxed);

        // A failure attributed to the CURRENT generation poisons.
        poison_sink_if_current(g_old);
        assert!(
            SINK_POISONED.load(Ordering::Relaxed),
            "a failure on the installed generation must poison"
        );

        // Simulate a re-registration: newer generation, poison cleared.
        let g_new = NEXT_SINK_GEN.fetch_add(1, Ordering::Relaxed);
        *lock.lock().unwrap() = Some(Sink {
            cb: ok_cb,
            ctx: 0,
            gen: g_new,
        });
        SINK_POISONED.store(false, Ordering::Relaxed);

        // A LATE failure from the OLD generation must NOT poison the new sink.
        poison_sink_if_current(g_old);
        assert!(
            !SINK_POISONED.load(Ordering::Relaxed),
            "an old callback's unwind must not poison a newer registration"
        );

        // Restore the process-global sink state for any other test.
        *lock.lock().unwrap() = None;
        SINK_POISONED.store(false, Ordering::Relaxed);
    }

    #[test]
    fn span_callsites_cache_off_even_on_kernel_targets() {
        // The bridge is event-only, so SPAN callsites never dispatch — even on
        // kernel targets — so the kernel's own `databricks*` spans (Session's
        // info_span!, the #[instrument] on next_batch) don't pay span machinery
        // for output that never comes.
        assert!(callsite_interest_for("databricks::sql::kernel", false).is_never());
        assert!(
            callsite_interest_for("databricks_sql_kernel::reader::inline::streaming", false)
                .is_never()
        );
        // Foreign spans stay off too.
        assert!(callsite_interest_for("hyper::client::pool", false).is_never());
    }

    #[test]
    fn visitor_combines_message_and_fields() {
        let mut v = MessageVisitor {
            message: "hello".to_owned(),
            ..Default::default()
        };
        v.push_field("statement_id", "abc-123".to_owned());
        assert_eq!(v.finish(), "hello statement_id=abc-123");
    }

    /// Drift guard: the bridge routes on the `databricks` prefix; the kernel
    /// stamps every `klog!` with `KERNEL_LOG_TARGET`. If that constant ever
    /// changed to a non-`databricks` prefix, kernel logs would silently stop
    /// reaching the callback — pin the two together.
    #[test]
    fn filter_prefix_still_matches_kernel_emit_target() {
        assert!(is_kernel_target(crate::klog::KERNEL_LOG_TARGET));
    }

    // NOTE: the end-to-end "MessageVisitor renders a REAL tracing event" test
    // lives in `tests/v0_c_abi_log_callback.rs` (a separate binary), NOT here.
    // Installing any `tracing` subscriber (even a thread-local `with_default`)
    // perturbs the `tracing/log` bridge's global max-level, which the `klog`
    // unit tests depend on being untouched — so a subscriber-installing test in
    // THIS binary makes `klog::tests` flaky by ordering. This mirrors the
    // CONSTRAINT note in `klog.rs`. The pure-predicate tests above (severity /
    // should_emit / visitor field-combining / target prefix) need no subscriber
    // and stay here.
}
