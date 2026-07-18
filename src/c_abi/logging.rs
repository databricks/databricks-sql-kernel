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
//!   `databricks*` events, filters by an atomic level, formats each into a
//!   [`Record`], and pushes it onto a **bounded** channel (drop-on-full — logs
//!   are advisory and must never block or back-pressure a kernel hot path).
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
use std::sync::{Mutex, Once, OnceLock};
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
}

const CHANNEL_CAPACITY: usize = 4096;

const LEVEL_OFF: u8 = 0;
const LEVEL_ERROR: u8 = 1;
const LEVEL_WARN: u8 = 2;
const LEVEL_INFO: u8 = 3;
const LEVEL_DEBUG: u8 = 4;
const LEVEL_TRACE: u8 = 5;

/// Numeric severity, low = more severe. `0` means "off / emit nothing".
static LEVEL: AtomicU8 = AtomicU8::new(LEVEL_WARN);

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
/// Won the subscriber but the OS refused to start the drain thread
/// (EAGAIN/ENOMEM) — a real failure the host should surface distinctly.
const INSTALL_SPAWN_FAILED: u8 = 3;

/// Outcome of the one-time subscriber install. Anything other than `INSTALL_OK`
/// means kernel logs won't reach the callback; the two failure discriminants
/// let the caller tell the benign already-installed case from a spawn failure.
static INSTALL_STATUS: AtomicU8 = AtomicU8::new(INSTALL_PENDING);

/// The drain-thread spawn error string, populated only on `INSTALL_SPAWN_FAILED`
/// so the returned `Internal` can carry the underlying OS error for triage.
static SPAWN_ERROR: OnceLock<Mutex<Option<String>>> = OnceLock::new();

/// Producer end, published once after the drain thread is confirmed running so
/// a live producer never outlives its consumer.
static SENDER: OnceLock<SyncSender<Record>> = OnceLock::new();

/// Current sink. Last-writer-wins; read by the drain thread per record.
static SINK: OnceLock<Mutex<Option<Sink>>> = OnceLock::new();

/// Guards the one-time subscriber install + channel + drain-thread spawn.
static INSTALL: Once = Once::new();

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

/// Derive the callback's level from a `RUST_LOG`-style directive string, matching
/// the level the stderr path (`kernel_init_logging`) would apply so the two routes
/// filter identically. Recognises a bare level (`debug`), the kernel target's
/// directive (`databricks::sql::kernel=debug` / `databricks_sql_kernel=debug`), or
/// a global fallback among comma-separated directives. Returns `None` (keep the
/// current default) if nothing matches.
///
/// When several directives match, this mirrors `EnvFilter` specificity rather
/// than taking the first one listed: `EnvFilter` applies a target directive to
/// an event only when the directive's target is a **prefix of the event's
/// target** (`meta.target().starts_with(directive_target)`), and among the
/// matching directives the **most specific** (longest target) wins, independent
/// of order. So `RUST_LOG=databricks=warn,databricks::sql::kernel=debug`
/// resolves to `debug` for a `databricks::sql::kernel` event — the same
/// threshold `kernel_init_logging`'s `EnvFilter` would apply — not the
/// first-listed `warn`. On a specificity tie the later directive wins
/// (`EnvFilter` dedups identical directives last-writer-wins). A matching
/// target directive always beats a bare global level, since a target-less
/// directive is the least specific match `EnvFilter` recognises.
///
/// Applicability is judged against the kernel's canonical target roots (see
/// [`directive_targets_kernel`]), NOT a bare `starts_with("databricks")`: a
/// longer non-matching sibling such as `databricksfoo=warn` or
/// `databricks::sql::kernel::foo=trace` is a prefix of no kernel event, so
/// `EnvFilter` skips it — and so must this, or it would wrongly outrank a real
/// `databricks=debug`. (This resolves a single scalar for the whole bridge, so
/// a directive naming a specific sub-module deeper than a root — e.g.
/// `databricks_sql_kernel::session=trace` — is conservatively treated as
/// non-selecting; the dominant `klog!` target `databricks::sql::kernel` is
/// resolved exactly.)
fn level_from_rust_log(rust_log: &str) -> Option<u8> {
    // Best kernel-applicable target directive seen so far, as (target length
    // used as the specificity key, resolved level).
    let mut best_target: Option<(usize, u8)> = None;
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
                        // Longest target = most specific; on a tie the later
                        // directive wins (matches `EnvFilter`'s last-writer
                        // dedup), so replace on `>=`.
                        let specificity = target.len();
                        let wins = match best_target {
                            Some((len, _)) => specificity >= len,
                            None => true,
                        };
                        if wins {
                            best_target = Some((specificity, l));
                        }
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
    // A matching target directive is always more specific than a bare global
    // level, so it wins when both are present.
    best_target.map(|(_, l)| l).or(global)
}

/// The kernel's canonical `tracing` target roots: the `klog!` target
/// ([`KERNEL_LOG_TARGET`](crate::klog::KERNEL_LOG_TARGET),
/// `databricks::sql::kernel`) and the crate's module-path root
/// (`databricks_sql_kernel`). A `RUST_LOG` target directive selects the kernel
/// only when `EnvFilter` would apply it, i.e. its target is a prefix of a
/// target the kernel actually emits at — so its target must be a prefix of one
/// of these roots.
const KERNEL_TARGET_ROOTS: [&str; 2] = [crate::klog::KERNEL_LOG_TARGET, "databricks_sql_kernel"];

/// Whether a `RUST_LOG` directive target names (a prefix of) a kernel target,
/// so `EnvFilter` would apply it to kernel events. Mirrors `EnvFilter`'s
/// `event_target.starts_with(directive_target)` against the canonical kernel
/// roots: a directive target that is a proper extension of a root
/// (`databricks::sql::kernel::foo`) or an unrelated `databricks`-prefixed
/// sibling (`databricksfoo`) matches no kernel event and is rejected.
fn directive_targets_kernel(target: &str) -> bool {
    KERNEL_TARGET_ROOTS
        .iter()
        .any(|root| root.starts_with(target))
}

/// Returns whether an event at `level` should currently be forwarded.
fn should_emit(level: &Level) -> bool {
    let configured = LEVEL.load(Ordering::Relaxed);
    configured != LEVEL_OFF && severity(level) <= configured
}

/// Targets the bridge forwards: the `klog!` shim's `databricks::sql::kernel`
/// and kernel module paths (`databricks_sql_kernel::*`). A raw `starts_with`
/// (not a `::`-segmented filter) so the underscore module-path variants match.
fn is_kernel_target(target: &str) -> bool {
    target.starts_with("databricks")
}

/// The configured level as a `tracing` [`LevelFilter`], for `max_level_hint` so
/// the process-global gate tracks the kernel level instead of pinning at TRACE.
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
        let Some(tx) = SENDER.get() else {
            return;
        };
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

/// Drain loop (own OS thread): pull one record at a time and invoke the host
/// callback with borrowed C strings. Runs off the kernel's tokio workers so a
/// slow callback can't stall query execution.
fn drain_loop(rx: Receiver<Record>) {
    // Drop count already reported to the host, so we only emit a synthetic
    // "dropped N" notice for the delta since the last delivered record.
    let mut reported_drops: u64 = 0;
    // `recv` errors only once the (process-global) sender is dropped, which
    // never happens in practice; exit cleanly if the channel is torn down.
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
        // Copy the sink out under the lock (cheap: a fn pointer + usize) so the
        // callback runs without holding the mutex — a reentrant setter can't
        // deadlock, and a slow callback can't block a level/sink update.
        let sink = match sink_lock.lock() {
            Ok(g) => g.as_ref().map(|s| (s.cb, s.ctx)),
            Err(_) => None,
        };
        let Some((cb, ctx)) = sink else {
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
                SINK_POISONED.store(true, Ordering::Relaxed);
                continue;
            }
        }
        if !invoke_callback(cb, ctx, record.level, record.target, &record.message) {
            SINK_POISONED.store(true, Ordering::Relaxed);
        }
    }
}

/// The kernel log target as a plain `&str`, for the synthetic drop-notice line
/// so it carries the same target as real kernel events.
fn is_kernel_target_str() -> &'static str {
    crate::klog::KERNEL_LOG_TARGET
}

/// Resolve the callback's numeric level from the caller's explicit `level`
/// argument, mirroring `kernel_init_logging`'s precedence so the two C-ABI
/// logging routes filter identically:
///
///   1. an explicit, recognised `level` word wins (e.g. Go's driver level
///      mapped through `DATABRICKS_LOG_LEVEL`);
///   2. otherwise a `RUST_LOG` directive (bare or `databricks*=…`);
///   3. otherwise the current default (`warn`), left untouched.
///
/// This is what keeps the callback on the one-knob contract PECOBLR-3650
/// established: `kernel_init_logging` maps the driver level to a level string,
/// and the callback path must honour that same string rather than only
/// `RUST_LOG` — otherwise a host at `DATABRICKS_LOG_LEVEL=debug` (but no
/// `RUST_LOG`) would silently drop kernel debug lines.
///
/// Note this resolves the same *level*, but the target matching differs from
/// `kernel_init_logging`: that path builds an `EnvFilter` (`::`-segment match on
/// `databricks::sql::kernel`), whereas the layer here matches the broader
/// `databricks` prefix (see `is_kernel_target`). So for a `RUST_LOG` target
/// directive the two routes select the same threshold but not necessarily the
/// identical event set.
fn resolve_callback_level(explicit: Option<&str>) -> Option<u8> {
    if let Some(word) = explicit {
        // A recognised explicit level always wins. "off" is honoured (the host
        // asked for silence); an unrecognised word falls through to RUST_LOG.
        if let Some(l) = parse_level_word(word) {
            return Some(l);
        }
    }
    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if let Some(l) = level_from_rust_log(&rust_log) {
            return Some(l);
        }
    }
    None
}

/// Install (process-wide) a callback that receives kernel `databricks*`
/// `tracing` events. `ctx` is passed back verbatim on every call. Passing a
/// null `cb` is rejected with `InvalidArgument` (to detach a sink, the design
/// would grow an explicit detach — v0 keeps install-only).
///
/// `level` (a NUL-terminated `OFF`/`ERROR`/`WARN`/`INFO`/`DEBUG`/`TRACE`
/// string, case-insensitive, or NULL) sets the forward threshold with the same
/// precedence as `kernel_init_logging`: an explicit recognised level wins, else
/// `RUST_LOG`. A host that maps its own logger level (Go's
/// `DATABRICKS_LOG_LEVEL`) passes it here so the callback filters on the one
/// knob rather than only `RUST_LOG`. When neither resolves (NULL / unrecognised
/// level AND no matching `RUST_LOG`) the current threshold is LEFT UNCHANGED —
/// which is the static `warn` default on the first-ever call, but the
/// previously-set level on a re-registration (the level is process-global and
/// last-writer-wins only when a writer actually resolves one).
///
/// Returns `Internal` (with a stored last error) if a global `tracing`
/// subscriber was already installed (e.g. by `kernel_init_logging`) — the two
/// are mutually exclusive; a C consumer installs exactly one.
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
    // Track whether the level actually changed so we only rebuild the (process-
    // wide, lock-taking) callsite interest cache when it matters — a same-level
    // ctx refresh or a call that loses the subscriber race shouldn't serialize
    // all logging dispatch.
    let mut level_changed = false;
    if let Some(resolved) = resolve_callback_level(explicit_level) {
        level_changed = LEVEL.swap(resolved, Ordering::Relaxed) != resolved;
    }

    // Record / refresh the sink (last writer wins). Clear any prior poison: a
    // callback that unwound poisons the sink (drain_loop stops delivering to
    // protect the process), but registering a NEW callback is an explicit
    // request for a fresh start, so honour the last-writer-wins contract and let
    // delivery resume. The drain thread reads the flag per record, so clearing
    // it here (before releasing the SINK lock) is immediately effective.
    let sink = SINK.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = sink.lock() {
        *guard = Some(Sink {
            cb,
            ctx: ctx as usize,
        });
        SINK_POISONED.store(false, Ordering::Relaxed);
    }

    // Install the process-global subscriber + drain thread exactly once.
    //
    // Order matters: attempt `set_global_default` FIRST, and only spawn the
    // drain thread if we win the one global subscriber. The common sequence is
    // "host called kernel_init_logging, a layer later calls this" — there we
    // lose the race and must NOT leave a drain thread parked on recv() forever
    // (an idle-thread leak). We use a plain Once-guarded closure rather than the
    // channel/thread first.
    let mut did_install = false;
    INSTALL.call_once(|| {
        did_install = true;
        let subscriber =
            tracing_subscriber::registry().with(KernelLogLayer.with_filter(KernelFilter));
        if tracing::subscriber::set_global_default(subscriber).is_err() {
            // Another global subscriber (e.g. kernel_init_logging) won. Do not
            // spawn a drain thread — SENDER stays unset, so on_event's `let Some
            // (tx)` short-circuits and nothing is enqueued.
            INSTALL_STATUS.store(INSTALL_ALREADY_SET, Ordering::Release);
            return;
        }
        // We own the subscriber; now stand up the delivery path. Spawn the drain
        // thread and publish SENDER only after it is confirmed running, so a
        // live producer never outlives its consumer.
        let (tx, rx) = sync_channel::<Record>(CHANNEL_CAPACITY);
        match std::thread::Builder::new()
            .name("kernel-log-drain".to_owned())
            .spawn(move || drain_loop(rx))
        {
            Ok(_) => {
                let _ = SENDER.set(tx);
                INSTALL_STATUS.store(INSTALL_OK, Ordering::Release);
            }
            Err(e) => {
                // We hold the subscriber but have no consumer; leave SENDER
                // unset (events short-circuit). Record the OS error distinctly
                // so on-call can tell a spawn failure (EAGAIN/ENOMEM) from the
                // benign "already installed" case. Use `.lock().ok()` (not
                // `.unwrap()`) so a poisoned mutex can't panic across the FFI
                // boundary — the message is best-effort diagnostics.
                if let Ok(mut guard) = SPAWN_ERROR.get_or_init(|| Mutex::new(None)).lock() {
                    *guard = Some(e.to_string());
                }
                INSTALL_STATUS.store(INSTALL_SPAWN_FAILED, Ordering::Release);
            }
        }
    });
    // Rebuild the process-wide callsite interest cache only when it can matter:
    // on the install that set our filter, or when a later call changed LEVEL.
    // set_global_default rebuilds on its own, but the level may have been set
    // before install, and a re-level after install needs a rebuild too; a
    // same-level ctx refresh or a race-loss does not (the rebuild takes the
    // global callsite lock and re-evaluates every callsite in the binary).
    if did_install || level_changed {
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
        // INSTALL_ALREADY_SET (or a stale PENDING, which can't happen after
        // call_once): a global subscriber was already installed.
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
/// # Safety
/// Each non-null out-param must point to a writable `bool` / `uint64_t`.
#[no_mangle]
pub unsafe extern "C" fn kernel_logging_stats(installed: *mut bool, dropped: *mut u64) {
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
    fn level_from_rust_log_reads_kernel_and_global_directives() {
        // The kernel target's own directive (either spelling) wins.
        assert_eq!(
            level_from_rust_log("databricks::sql::kernel=debug"),
            Some(LEVEL_DEBUG)
        );
        assert_eq!(
            level_from_rust_log("databricks_sql_kernel=trace"),
            Some(LEVEL_TRACE)
        );
        // A kernel directive still wins when mixed with foreign ones.
        assert_eq!(
            level_from_rust_log("hyper=off,databricks::sql::kernel=info,rustls=warn"),
            Some(LEVEL_INFO)
        );
        // A bare global level applies when no kernel directive is present.
        assert_eq!(level_from_rust_log("debug"), Some(LEVEL_DEBUG));
        // Nothing recognisable → None (keep the default).
        assert_eq!(level_from_rust_log("hyper=warn"), None);
        assert_eq!(level_from_rust_log(""), None);
    }

    #[test]
    fn level_from_rust_log_mirrors_envfilter_specificity() {
        // With a broad and a specific `databricks*` directive both present, the
        // MORE SPECIFIC (longer target) one wins — matching `EnvFilter`, not
        // first-match-wins — and the result is order-independent.
        assert_eq!(
            level_from_rust_log("databricks=warn,databricks::sql::kernel=debug"),
            Some(LEVEL_DEBUG),
            "the specific kernel directive must win over the broad databricks=warn"
        );
        assert_eq!(
            level_from_rust_log("databricks::sql::kernel=debug,databricks=warn"),
            Some(LEVEL_DEBUG),
            "resolution must not depend on directive order"
        );
        // Same, for the underscore module-path spelling.
        assert_eq!(
            level_from_rust_log("databricks=warn,databricks_sql_kernel=trace"),
            Some(LEVEL_TRACE)
        );
        assert_eq!(
            level_from_rust_log("databricks_sql_kernel=trace,databricks=warn"),
            Some(LEVEL_TRACE)
        );
        // A targeted `databricks*` directive beats a bare global level in either
        // order (a target-less directive is the least specific match).
        assert_eq!(
            level_from_rust_log("info,databricks=error"),
            Some(LEVEL_ERROR)
        );
        assert_eq!(
            level_from_rust_log("databricks=error,info"),
            Some(LEVEL_ERROR)
        );
    }

    #[test]
    fn level_from_rust_log_ignores_non_matching_databricks_siblings() {
        // `EnvFilter` applies a target directive only when its target is a
        // prefix of the event target. A longer `databricks`-prefixed sibling
        // that matches NO kernel target must NOT outrank a real `databricks=`
        // directive just because it's longer.
        assert_eq!(
            level_from_rust_log("databricksfoo=warn,databricks=debug"),
            Some(LEVEL_DEBUG),
            "an unrelated databricks-prefixed sibling must be ignored"
        );
        // A directive DEEPER than the klog target matches no emitted event
        // (klog stamps exactly `databricks::sql::kernel`), so it's skipped and
        // the broad `databricks=debug` applies.
        assert_eq!(
            level_from_rust_log("databricks::sql::kernel::foo=trace,databricks=debug"),
            Some(LEVEL_DEBUG),
            "a directive deeper than the kernel target must be ignored"
        );
        // A non-matching sibling as the ONLY databricks directive must fall back
        // to the bare global level (not the sibling's level).
        assert_eq!(
            level_from_rust_log("databricksfoo=trace,info"),
            Some(LEVEL_INFO)
        );
        // Two equal-length siblings that match no kernel root resolve to None
        // (keep the default) rather than letting order pick a bogus level.
        assert_eq!(
            level_from_rust_log("databricks::aa=warn,databricks::bb=debug"),
            None
        );
    }

    #[test]
    fn directive_targets_kernel_matches_prefixes_of_kernel_roots() {
        // Prefixes of the klog target and the module-path root are accepted.
        assert!(directive_targets_kernel("databricks"));
        assert!(directive_targets_kernel("databricks::sql"));
        assert!(directive_targets_kernel(crate::klog::KERNEL_LOG_TARGET));
        assert!(directive_targets_kernel("databricks_sql_kernel"));
        // Unrelated siblings and over-specific extensions are rejected.
        assert!(!directive_targets_kernel("databricksfoo"));
        assert!(!directive_targets_kernel("databricks::sql::kernel::foo"));
        assert!(!directive_targets_kernel("hyper"));
    }

    #[test]
    fn resolve_callback_level_prefers_explicit_over_rust_log() {
        // An explicit, recognised level wins outright — this is the one-knob
        // path (Go maps DATABRICKS_LOG_LEVEL and passes it in), independent of
        // whatever RUST_LOG happens to be. We don't touch the process RUST_LOG
        // here (that would race other tests); an explicit level short-circuits
        // before RUST_LOG is ever read.
        assert_eq!(resolve_callback_level(Some("debug")), Some(LEVEL_DEBUG));
        assert_eq!(resolve_callback_level(Some("OFF")), Some(LEVEL_OFF));
        assert_eq!(resolve_callback_level(Some("Error")), Some(LEVEL_ERROR));
        // An unrecognised explicit word falls through (to RUST_LOG, else None).
        // With no kernel/global RUST_LOG directive this yields None regardless
        // of the ambient value, so assert only the None-or-Some shape is sane by
        // checking a clearly-bogus word doesn't parse as a level itself.
        assert_eq!(parse_level_word("not-a-level"), None);
        // NULL/absent explicit level defers to RUST_LOG (covered by
        // level_from_rust_log tests); nothing recognisable → None (keep default).
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
