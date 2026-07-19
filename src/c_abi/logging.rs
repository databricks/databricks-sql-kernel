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
//!   `databricks*` events, filters by a single level (an explicit one-knob
//!   level, else the most-verbose kernel-applicable `RUST_LOG` level), formats
//!   each into a [`Record`], and pushes it onto a **bounded** channel
//!   (drop-on-full — logs are advisory and must never block or back-pressure a
//!   kernel hot path).
//! - A dedicated drain thread pulls records and invokes the C callback once per
//!   record with borrowed C strings (valid only for the call).
//!
//! ## Threading & safety contract (full version in `databricks_kernel.h`)
//!
//! The callback fires from the drain thread (never a kernel tokio worker), so
//! it must be thread-safe and must not call back into any `kernel_*` entry
//! point (the reentrancy guard only covers tokio workers, so a blocking
//! `kernel_*` call here would stall the drain thread). An unwinding callback is
//! contained by `catch_unwind` and poisons the sink rather than aborting the
//! process, but a well-behaved callback still must not unwind, since poisoning
//! stops delivery.
//!
//! ## Mutual exclusion with `kernel_init_logging`
//!
//! Both install the one process-global `tracing` subscriber; the first
//! `set_global_default` wins and the loser returns `Internal`, so a C consumer
//! picks one. The sink itself is last-writer-wins, refreshed on every call.

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
use std::sync::{Mutex, OnceLock};
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Filter, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// Host log sink: a C function pointer plus an opaque host context passed back
/// verbatim on every call. `level` is a `KernelLogLevel` value (1=ERROR..5=TRACE;
/// see the `LEVEL_*` constants and the header enum); `target` and `message` are
/// NUL-terminated C strings valid only for the duration of the call.
///
/// The ABI is `C-unwind` so a panicking host callback propagates across the
/// boundary and `invoke_callback`'s `catch_unwind` can contain it; a plain
/// `extern "C"` fn would instead abort the process. The C representation is
/// unchanged, and non-unwinding callbacks are unaffected.
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
    /// Monotonic registration id. The drain thread poisons the sink on an unwind
    /// only if this id is still installed, so an old callback that unwinds after
    /// a re-registration can't kill the replacement.
    gen: u64,
}

const CHANNEL_CAPACITY: usize = 4096;

// Numeric severity delivered to the host callback and used as the forward
// threshold (low = more severe; `0` = off / emit nothing). These are the Rust
// side of the header's `KernelLogLevel` enum and MUST stay in lockstep with it
// (`KernelLogLevel_Off`..`KernelLogLevel_Trace`); `pub` so a consumer of the
// crate (and the C-ABI documentation) can reference them by name rather than
// hard-coding 1..5.
pub const LEVEL_OFF: u8 = 0;
pub const LEVEL_ERROR: u8 = 1;
pub const LEVEL_WARN: u8 = 2;
pub const LEVEL_INFO: u8 = 3;
pub const LEVEL_DEBUG: u8 = 4;
pub const LEVEL_TRACE: u8 = 5;

/// Forward threshold for all kernel events: the explicit one-knob level, the
/// most-verbose kernel-applicable `RUST_LOG` level, or the static `warn`
/// default. Numeric severity, low = more severe; `0` means "off / emit nothing".
static LEVEL: AtomicU8 = AtomicU8::new(LEVEL_WARN);

/// Count of log records dropped because the bounded channel was full or the
/// sink was poisoned. Surfaced via [`kernel_logging_stats`] so the loss is
/// observable, not silent.
static DROPPED: AtomicU64 = AtomicU64::new(0);

/// Set if the host callback ever unwound across the FFI boundary. A callback
/// that panics once is assumed to keep panicking, so the drain thread stops
/// delivering rather than re-invoke it. Surfaced via [`kernel_logging_stats`]
/// (`installed` reads `false` while poisoned), and cleared when a new callback
/// is registered so a host can recover by re-installing.
static SINK_POISONED: AtomicBool = AtomicBool::new(false);

const INSTALL_PENDING: u8 = 0;
const INSTALL_OK: u8 = 1;
/// Lost the one-global-subscriber race (e.g. `kernel_init_logging` won) — a
/// benign outcome; the host picks one of the two logging routes.
const INSTALL_ALREADY_SET: u8 = 2;
/// The OS refused to start the drain thread (EAGAIN/ENOMEM) before the
/// subscriber was claimed. Non-terminal: a later `kernel_set_log_callback`
/// retries, so a transient spawn failure isn't permanent.
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

/// Current sink. Last-writer-wins; read by the drain thread per record.
static SINK: OnceLock<Mutex<Option<Sink>>> = OnceLock::new();

/// Source of monotonic sink registration ids ([`Sink::gen`]). Bumped under the
/// `SINK` lock on every (re-)registration so the drain thread can tell whether
/// the callback it just invoked is still the installed one.
static NEXT_SINK_GEN: AtomicU64 = AtomicU64::new(0);

/// Serializes install attempts (subscriber + channel + drain-thread spawn). A
/// plain lock rather than a `Once` so a transient spawn failure stays retryable
/// via [`INSTALL_STATUS`]; `set_global_default` is still attempted at most once.
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

/// Resolve a `RUST_LOG`-style string to the single most-verbose level that
/// applies to kernel events, or `None` if nothing kernel-applicable is present.
///
/// The callback carries one level, so directives are collapsed by taking the
/// max (most verbose) across the bare global level and every kernel-applicable
/// target directive ([`directive_targets_kernel`]). A directive with no `=` is
/// a bare level (the global) or a bare kernel target (TRACE, as `EnvFilter`
/// reads it). Foreign directives (`hyper=warn`) and non-matching `databricks`
/// siblings are ignored. This can forward a superset of what the stderr path's
/// per-target `EnvFilter` would — acceptable for advisory logging, and a host
/// wanting exact per-target control uses `kernel_init_logging` instead.
fn parse_rust_log(rust_log: &str) -> Option<u8> {
    let mut level: Option<u8> = None;
    let mut bump = |l: u8| level = Some(level.map_or(l, |cur| cur.max(l)));
    for directive in rust_log.split(',') {
        let d = directive.trim();
        if d.is_empty() {
            continue;
        }
        match d.split_once('=') {
            Some((target, lvl)) => {
                if directive_targets_kernel(target.trim()) {
                    if let Some(l) = parse_level_word(lvl) {
                        bump(l);
                    }
                }
            }
            None => {
                if let Some(l) = parse_level_word(d) {
                    bump(l);
                } else if directive_targets_kernel(d) {
                    bump(LEVEL_TRACE);
                }
            }
        }
    }
    level
}

/// The crate's module-path root: the `#[instrument]` / default-target namespace
/// (`databricks_sql_kernel::*`) the bridge also forwards (see `is_kernel_target`).
const KERNEL_MODULE_ROOT: &str = "databricks_sql_kernel";

/// The kernel's canonical `tracing` target roots: the `klog!` target
/// ([`KERNEL_LOG_TARGET`](crate::klog::KERNEL_LOG_TARGET),
/// `databricks::sql::kernel`) and the crate's module-path root
/// ([`KERNEL_MODULE_ROOT`]).
const KERNEL_TARGET_ROOTS: [&str; 2] = [crate::klog::KERNEL_LOG_TARGET, KERNEL_MODULE_ROOT];

/// Whether a `RUST_LOG` directive target selects any event the bridge forwards,
/// under `EnvFilter`'s prefix match. True when the target is a prefix of a
/// canonical root (`databricks`, `databricks::sql`, `databricks_sql_kernel`, …)
/// or a `::`-boundary submodule of the module-path root
/// (`databricks_sql_kernel::session`). Uses the same segment-boundary helper as
/// [`is_kernel_target`], so a module-root sibling (`databricks_sql_kernel_ext`),
/// a non-root sibling (`databricksfoo`), and an over-specific extension of the
/// fixed-depth `klog!` target (`databricks::sql::kernel::foo`) are all rejected.
fn directive_targets_kernel(target: &str) -> bool {
    is_target_or_submodule(target, KERNEL_MODULE_ROOT)
        || KERNEL_TARGET_ROOTS
            .iter()
            .any(|root| root.starts_with(target))
}

/// Returns whether an event at `level` should currently be forwarded, against
/// the single configured [`LEVEL`] threshold.
fn should_emit(level: &Level) -> bool {
    let threshold = LEVEL.load(Ordering::Relaxed);
    threshold != LEVEL_OFF && severity(level) <= threshold
}

/// Whether `target` is exactly `root` or a `::`-delimited submodule of it — a
/// segment-boundary match, so a sibling like `databricks_sql_kernel_ext` does
/// not match `databricks_sql_kernel`.
fn is_target_or_submodule(target: &str, root: &str) -> bool {
    target
        .strip_prefix(root)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
}

/// Targets the bridge forwards: the `klog!` shim's `databricks::sql::kernel`
/// and the crate's module paths (`databricks_sql_kernel`), plus any submodule
/// of either. A segment-boundary match, so an unrelated `databricks`-prefixed
/// target from another crate is not routed into the kernel callback.
fn is_kernel_target(target: &str) -> bool {
    is_target_or_submodule(target, crate::klog::KERNEL_LOG_TARGET)
        || is_target_or_submodule(target, KERNEL_MODULE_ROOT)
}

/// The configured [`LEVEL`] as a `tracing` [`LevelFilter`], for `max_level_hint`
/// so the process-global gate tracks the kernel level rather than pinning at
/// TRACE.
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

/// Per-callsite interest. Kernel EVENT callsites re-evaluate per event
/// (`sometimes`, for the dynamic level); everything else caches off (`never`).
/// The bridge is event-only, so span callsites cache off even on kernel targets
/// — the kernel's own `databricks*` spans then pay no span machinery for output
/// this layer never produces.
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
///
/// The channel sender lives in the layer instance, so `set_global_default`
/// publishes the subscriber and its sender atomically —
/// [`on_event`](KernelLogLayer::on_event) never sees an installed-but-senderless
/// state that would drop a concurrent log uncounted.
struct KernelLogLayer {
    tx: SyncSender<Record>,
}

/// Per-layer filter gating the bridge: foreign callsites cache off, kernel
/// callsites re-evaluate per event, and `max_level_hint` tracks the level.
struct KernelFilter;

impl<S> Filter<S> for KernelFilter {
    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> Interest {
        callsite_interest_for(meta.target(), meta.is_event())
    }

    fn enabled(&self, meta: &Metadata<'_>, _ctx: &Context<'_, S>) -> bool {
        // Event-only: never enable spans (the layer has no span consumer).
        meta.is_event() && is_kernel_target(meta.target()) && should_emit(meta.level())
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
        // A poisoned sink drops everything until re-registration; short-circuit
        // before the field visit and enqueue so we don't format a record we'll
        // drop. Still count the loss (each record is counted exactly once — here
        // if produced while poisoned, or by the drain thread if already queued).
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
        // task on a slow host callback. Count the loss so it's observable via
        // kernel_logging_stats rather than vanishing silently.
        if self.tx.try_send(record).is_err() {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Invoke the host callback once, containing any unwind with `catch_unwind`
/// (the same guard every FFI boundary in the crate uses). Returns `false` if
/// the callback unwound, so the caller can poison the sink.
fn invoke_callback(
    cb: kernel_log_callback,
    ctx: usize,
    level: c_int,
    target: &str,
    message: &str,
) -> bool {
    // On the rare message with an interior NUL, truncate at the first NUL
    // (keeping the prefix) instead of dropping the whole string. The bytes
    // before the NUL can't contain one, so the rebuild is infallible.
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

/// Poison the sink — stop delivery — but only if generation `gen` is still the
/// installed registration. The drain thread runs the callback unlocked, so the
/// host may re-register (bumping the generation, clearing the poison) while an
/// old callback is mid-unwind; comparing under the `SINK` lock keeps that old
/// failure from killing the newer registration.
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
        // Copy the sink out under the lock so the callback runs without holding
        // the mutex — a reentrant setter can't deadlock and a slow callback
        // can't block a level/sink update.
        let sink = match sink_lock.lock() {
            Ok(g) => g.as_ref().map(|s| (s.cb, s.ctx, s.gen)),
            Err(_) => None,
        };
        let Some((cb, ctx, gen)) = sink else {
            continue;
        };
        if !invoke_callback(cb, ctx, record.level, record.target, &record.message) {
            // The callback unwound without delivering this record; count the
            // loss, then poison iff this callback is still the installed one.
            DROPPED.fetch_add(1, Ordering::Relaxed);
            poison_sink_if_current(gen);
        }
    }
}

/// Resolve the callback's forward level, mirroring `kernel_init_logging`'s
/// precedence: an explicit recognised `level` word wins (Go maps its driver
/// level through here); else the most-verbose kernel-applicable `RUST_LOG` level
/// ([`parse_rust_log`]); else `None`, leaving the current level untouched.
///
/// The explicit path matters because a host at `DATABRICKS_LOG_LEVEL=debug` with
/// no `RUST_LOG` must still get kernel debug lines.
fn resolve_level(explicit: Option<&str>) -> Option<u8> {
    explicit.and_then(parse_level_word).or_else(|| {
        std::env::var("RUST_LOG")
            .ok()
            .and_then(|r| parse_rust_log(&r))
    })
}

/// Install `level` as the process-global threshold, returning whether it changed
/// (so the caller only rebuilds the callsite interest cache when it matters).
fn apply_level(level: u8) -> bool {
    LEVEL.swap(level, Ordering::Relaxed) != level
}

/// Stand up the process-global subscriber + drain thread, retryable across
/// calls (see [`INSTALL_LOCK`]). Returns `true` iff THIS call performed the
/// successful install; a benign lost-race or a spawn failure returns `false`,
/// with the outcome recorded in [`INSTALL_STATUS`] for the caller's return code.
///
/// The drain thread is spawned FIRST, then the subscriber is claimed. So a
/// spawn failure (EAGAIN/ENOMEM) bails before touching the global slot, leaving
/// it free for `kernel_init_logging` and the status retryable; and a lost race
/// drops the layer (and its sender), so the freshly-spawned drain thread's
/// `recv()` errors and it exits without leaking. `set_global_default` runs at
/// most once per process — only after the drain thread is up, and both its
/// outcomes are terminal.
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
        // Record the OS error distinctly from the benign "already installed"
        // case. `.lock().ok()` so a poisoned mutex can't panic across the FFI
        // boundary; `tx` drops here, leaving the subscriber slot untouched.
        if let Ok(mut guard) = SPAWN_ERROR.get_or_init(|| Mutex::new(None)).lock() {
            *guard = Some(e.to_string());
        }
        INSTALL_STATUS.store(INSTALL_SPAWN_FAILED, Ordering::Release);
        return false;
    }
    // Drain thread is up; build the layer (holding the sender) and claim the
    // one global subscriber.
    let subscriber =
        tracing_subscriber::registry().with(KernelLogLayer { tx }.with_filter(KernelFilter));
    if tracing::subscriber::set_global_default(subscriber).is_err() {
        // Another subscriber won; the dropped layer takes its sender with it, so
        // the drain thread's `recv()` errors and it exits cleanly.
        INSTALL_STATUS.store(INSTALL_ALREADY_SET, Ordering::Release);
        return false;
    }
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
/// uniform threshold, else the single most-verbose kernel-applicable `RUST_LOG`
/// level (collapsed to one threshold, not per-target filtering — the bridge
/// carries one level). When neither resolves, the current filter is left
/// unchanged — the `warn` default on the first call, or the previously-set
/// filter on a re-registration.
///
/// On failure the return code encodes whether a retry could help, so a host
/// doesn't have to string-match the message: `Internal` (terminal,
/// `retryable=false`) if a global `tracing` subscriber was already installed
/// (they are mutually exclusive; install exactly one), or `ResourceExhausted`
/// (`retryable=true`) if the log-drain thread could not be spawned — a later
/// call attempts the install again.
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

    // A non-UTF-8 `level` is treated as absent (best-effort, like
    // kernel_init_logging's level arg) rather than a hard error.
    let explicit_level = if level.is_null() {
        None
    } else {
        std::ffi::CStr::from_ptr(level).to_str().ok()
    };
    // Track whether the level actually changed, so the lock-taking callsite
    // interest cache is only rebuilt when it matters (not on a ctx refresh).
    let mut filter_changed = false;
    if let Some(level) = resolve_level(explicit_level) {
        filter_changed = apply_level(level);
    }

    // Record / refresh the sink (last writer wins) and clear any prior poison:
    // registering a new callback is an explicit request to resume delivery.
    let sink = SINK.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = sink.lock() {
        // A fresh generation so an old in-flight callback's unwind can't poison
        // this new registration (see poison_sink_if_current).
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
    // Rebuild the callsite interest cache only when it can matter: on the
    // install that set our filter, or a later call that changed the filter. The
    // rebuild takes the global callsite lock, so skip it on a ctx-only refresh.
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
            // `ResourceExhausted` (retryable=true) not `Internal` (retryable=false):
            // an OS refusal to spawn the drain thread (EAGAIN/ENOMEM) is transient
            // and a later call retries the install, so the machine-readable
            // `KernelError.retryable` must say so — distinct from the terminal
            // already-installed case below, which stays a non-retryable `Internal`.
            crate::c_abi::error::store_and_code(Error::resource_exhausted().message(format!(
                "kernel_set_log_callback: the log-drain thread could not be started ({detail}); \
                     kernel logs will not reach the callback"
            )))
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

/// Report the log-callback bridge's health (full contract in
/// `databricks_kernel.h`). `installed` is `true` iff this callback won the
/// subscriber and the sink is not poisoned; `dropped` is the cumulative records
/// lost to channel overflow or poisoning. A NULL out-param is skipped, and it
/// never fails.
///
/// Clears the thread-local last-error on entry like every other C-ABI entry
/// point, so a host polling this infallible accessor between a failed call and
/// reading the error doesn't observe a stale error through it.
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
        LEVEL.store(LEVEL_INFO, Ordering::Relaxed);
        assert!(should_emit(&Level::ERROR));
        assert!(should_emit(&Level::INFO));
        assert!(!should_emit(&Level::DEBUG));
        LEVEL.store(LEVEL_OFF, Ordering::Relaxed);
        assert!(!should_emit(&Level::ERROR));
        LEVEL.store(LEVEL_WARN, Ordering::Relaxed); // restore
    }

    #[test]
    fn parse_rust_log_collapses_to_most_verbose_kernel_level() {
        // Foreign directives are ignored; the kernel one sets the level.
        assert_eq!(
            parse_rust_log("hyper=off,databricks::sql::kernel=info,rustls=warn"),
            Some(LEVEL_INFO)
        );
        // Either kernel spelling is accepted.
        assert_eq!(
            parse_rust_log("databricks_sql_kernel=trace"),
            Some(LEVEL_TRACE)
        );
        // A bare level word applies.
        assert_eq!(parse_rust_log("debug"), Some(LEVEL_DEBUG));
        // Nothing kernel-applicable and no bare level → None.
        assert_eq!(parse_rust_log("hyper=warn"), None);
        assert_eq!(parse_rust_log(""), None);
        // Non-matching databricks siblings and over-specific klog extensions are
        // ignored (prefixes of no kernel event).
        assert_eq!(
            parse_rust_log("databricksfoo=warn,databricks::sql::kernel::foo=trace"),
            None
        );
    }

    #[test]
    fn parse_rust_log_target_only_directive_is_trace() {
        // `EnvFilter` reads a bare kernel target (no `=level`) as that target at
        // TRACE — it must NOT be ignored, which would leave the callback at WARN
        // where kernel_init_logging emits trace.
        assert_eq!(parse_rust_log("databricks::sql::kernel"), Some(LEVEL_TRACE));
        // A foreign target-only directive is ignored; a bare kernel target is
        // TRACE and a bare level word is INFO, so the max (TRACE) wins.
        assert_eq!(
            parse_rust_log("hyper,databricks_sql_kernel::session,info"),
            Some(LEVEL_TRACE)
        );
    }

    #[test]
    fn parse_rust_log_takes_max_across_directives() {
        // Multiple kernel-applicable directives collapse to the most verbose,
        // order-independent (the callback carries one level). ERROR + TRACE → TRACE.
        assert_eq!(
            parse_rust_log("databricks::sql::kernel=error,databricks_sql_kernel::reader=trace"),
            Some(LEVEL_TRACE)
        );
        assert_eq!(
            parse_rust_log("databricks_sql_kernel::reader=trace,databricks::sql::kernel=error"),
            Some(LEVEL_TRACE)
        );
        // A bare global level is also folded into the max.
        assert_eq!(
            parse_rust_log("warn,databricks::sql::kernel=debug"),
            Some(LEVEL_DEBUG)
        );
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
        // A module-root SIBLING (not a `::`-boundary submodule) is rejected —
        // it raw-prefixes `databricks_sql_kernel` but matches no forwarded event
        // (this is the segment-boundary alignment with is_kernel_target).
        assert!(!directive_targets_kernel("databricks_sql_kernel_ext"));
    }

    #[test]
    fn resolve_level_explicit_level_wins() {
        // An explicit, recognised level wins outright — the one-knob path (Go
        // maps DATABRICKS_LOG_LEVEL and passes it in), independent of RUST_LOG.
        // We don't touch the process RUST_LOG here (that would race other tests);
        // an explicit level short-circuits before RUST_LOG is ever read.
        assert_eq!(resolve_level(Some("debug")), Some(LEVEL_DEBUG));
        assert_eq!(resolve_level(Some("OFF")), Some(LEVEL_OFF));
        assert_eq!(resolve_level(Some("Error")), Some(LEVEL_ERROR));
        // An unrecognised explicit word falls through (to RUST_LOG, else None).
        assert_eq!(parse_level_word("not-a-level"), None);
        // NULL/absent explicit level defers to RUST_LOG (covered by the
        // parse_rust_log tests).
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

    /// Drift guard: the bridge routes kernel events by matching the `klog!`
    /// target root; the kernel stamps every `klog!` with `KERNEL_LOG_TARGET`. If
    /// that constant ever changed out from under the routing roots, kernel logs
    /// would silently stop reaching the callback — pin the two together.
    #[test]
    fn filter_prefix_still_matches_kernel_emit_target() {
        assert!(is_kernel_target(crate::klog::KERNEL_LOG_TARGET));
    }

    #[test]
    fn is_kernel_target_matches_only_roots_and_submodules() {
        // The kernel's own targets — exact roots and their `::` submodules — are
        // forwarded.
        assert!(is_kernel_target(crate::klog::KERNEL_LOG_TARGET));
        assert!(is_kernel_target("databricks::sql::kernel::reader"));
        assert!(is_kernel_target("databricks_sql_kernel"));
        assert!(is_kernel_target("databricks_sql_kernel::session"));
        assert!(is_kernel_target("databricks_sql_kernel::reader::inline"));
        // Unrelated `databricks`-prefixed targets are NOT forwarded: matching is
        // on a `::` SEGMENT boundary, not a raw `starts_with("databricks")`, so a
        // sibling crate/library log can't be routed into the kernel callback.
        assert!(!is_kernel_target("databricksfoo"));
        assert!(!is_kernel_target("databricks_sql_kernel_ext"));
        assert!(!is_kernel_target("databricks::sql::other"));
        assert!(!is_kernel_target("databricks"));
        assert!(!is_kernel_target("hyper"));
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
