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

//! End-to-end delivery test for the `kernel_set_log_callback` reverse-call log
//! bridge. Registers a C callback, emits kernel `tracing` events on the
//! contract target, and asserts the callback received them with the message and
//! target intact — and that the `level` argument gates delivery in both
//! directions (a restrictive level filters a below-threshold event; a
//! permissive level delivers an event below the default threshold).
//!
//! This lives in its own integration-test binary (NOT a `#[cfg(test)] mod` in
//! `src/c_abi/logging.rs`) on purpose: installing the process-global `tracing`
//! subscriber the bridge needs perturbs the `tracing/log` bridge's global
//! max-level, which the `klog` unit tests depend on being untouched — so a
//! subscriber-installing test in the lib-test binary makes `klog::tests` flaky
//! by ordering. A separate binary has its own global state. Mirrors the
//! CONSTRAINT note in `src/klog.rs`.

use std::os::raw::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use databricks_sql_kernel::c_abi::error::{kernel_get_last_error, KernelError};
use databricks_sql_kernel::c_abi::logging::{kernel_logging_stats, kernel_set_log_callback};
use databricks_sql_kernel::c_abi::KernelStatusCode;

// Captured (level, target, message) tuples, keyed via a process-global slot the
// callback pushes into.
static CAPTURED: Mutex<Vec<(c_int, String, String)>> = Mutex::new(Vec::new());
static CALLS: AtomicUsize = AtomicUsize::new(0);
// The `ctx` the kernel passed back on the most recent call, recorded as a usize
// so we can assert the round-trip returns the exact pointer we registered.
static SEEN_CTX: AtomicUsize = AtomicUsize::new(0);

extern "C-unwind" fn capture_cb(
    ctx: *mut c_void,
    level: c_int,
    target: *const c_char,
    message: *const c_char,
) {
    CALLS.fetch_add(1, Ordering::SeqCst);
    SEEN_CTX.store(ctx as usize, Ordering::SeqCst);
    // SAFETY: the bridge passes valid NUL-terminated C strings for the call.
    let target = unsafe { std::ffi::CStr::from_ptr(target) }
        .to_string_lossy()
        .into_owned();
    let message = unsafe { std::ffi::CStr::from_ptr(message) }
        .to_string_lossy()
        .into_owned();
    CAPTURED.lock().unwrap().push((level, target, message));
}

/// A callback that always unwinds across the FFI boundary. The bridge's
/// `catch_unwind` must contain it, count the undelivered record, and poison the
/// sink (stop delivery) rather than abort the process.
extern "C-unwind" fn panic_cb(
    _ctx: *mut c_void,
    _level: c_int,
    _target: *const c_char,
    _message: *const c_char,
) {
    panic!("callback boom");
}

/// Poll up to ~1s for the captured records to satisfy `pred`, returning the
/// final snapshot either way (delivery is async via the bridge's drain thread).
fn wait_until(pred: impl Fn(&[(c_int, String, String)]) -> bool) -> Vec<(c_int, String, String)> {
    for _ in 0..100 {
        let got = CAPTURED.lock().unwrap().clone();
        if pred(&got) {
            return got;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    CAPTURED.lock().unwrap().clone()
}

/// Whether a kernel-target record carrying `marker` in its message was captured.
fn saw_marker(got: &[(c_int, String, String)], marker: &str) -> bool {
    got.iter()
        .any(|(_, t, m)| t == databricks_sql_kernel::KERNEL_LOG_TARGET && m.contains(marker))
}

#[test]
fn log_callback_delivers_and_level_gates_end_to_end() {
    // Register a real (non-null) ctx so we can assert the round-trip returns the
    // exact pointer we passed — a stand-in for the Go cgo.Handle token.
    let ctx_marker: Box<u64> = Box::new(0xC0FFEE);
    let ctx_ptr = Box::into_raw(ctx_marker) as *mut c_void;

    // === Step 1: a RESTRICTIVE level must FILTER a below-threshold event. ===
    // Install at ERROR. A WARN event must not be delivered — this is what proves
    // the `level` argument actually gates delivery (the previous "trace + WARN"
    // shape would pass even if `level` were ignored, since WARN clears the
    // default WARN threshold). This is a fresh, single-test binary, so nothing
    // else has installed a global subscriber: install MUST succeed. Accepting
    // `Internal` here would let a regression that never installs stay green.
    let level_error = std::ffi::CString::new("error").unwrap();
    let rc = unsafe { kernel_set_log_callback(Some(capture_cb), ctx_ptr, level_error.as_ptr()) };
    assert_eq!(
        rc,
        KernelStatusCode::Success,
        "kernel_set_log_callback must succeed on first install in a fresh binary; got {rc:?}"
    );

    let filtered_marker = "probe-warn-should-be-filtered-a1";
    let barrier_marker = "probe-error-barrier-b2";
    // Emit the (to-be-filtered) WARN first, then an ERROR barrier. The single
    // drain thread delivers FIFO, so once the ERROR barrier lands we know the
    // earlier WARN would already have arrived had it passed the filter — a
    // deterministic negative assertion, not a "sleep and hope".
    tracing::warn!(target: databricks_sql_kernel::KERNEL_LOG_TARGET, "{}", filtered_marker);
    tracing::error!(target: databricks_sql_kernel::KERNEL_LOG_TARGET, "{}", barrier_marker);
    let got = wait_until(|g| saw_marker(g, barrier_marker));
    assert!(
        saw_marker(&got, barrier_marker),
        "the ERROR barrier event must be delivered at level=error; captured={got:?}"
    );
    assert!(
        !saw_marker(&got, filtered_marker),
        "a WARN event must NOT be delivered when level=error — this proves the level \
         argument gates delivery; captured={got:?}"
    );

    // === Step 2: a PERMISSIVE level must DELIVER a below-default event. ===
    // Re-register at TRACE and emit a DEBUG event. DEBUG is below the static
    // WARN default, so its delivery proves the `level` argument also loosens the
    // threshold (not just that WARN happens to clear the default). Same ctx, so
    // the round-trip assertion below still holds.
    let level_trace = std::ffi::CString::new("trace").unwrap();
    let rc = unsafe { kernel_set_log_callback(Some(capture_cb), ctx_ptr, level_trace.as_ptr()) };
    assert_eq!(
        rc,
        KernelStatusCode::Success,
        "re-registering the callback must succeed; got {rc:?}"
    );

    // `klog::debug!` is pub(crate); from this external test binary we emit the
    // exact `tracing` event it expands to (same target + event field).
    let stmt = "stmt-cb-probe-9c1";
    tracing::debug!(
        target: databricks_sql_kernel::KERNEL_LOG_TARGET,
        statement_id = stmt,
        "callback probe message"
    );
    let got = wait_until(|g| saw_marker(g, stmt));
    let hit = got
        .iter()
        .find(|(_, t, m)| t == databricks_sql_kernel::KERNEL_LOG_TARGET && m.contains(stmt));
    let (_level, _target, message) = hit.unwrap_or_else(|| {
        panic!(
            "a DEBUG kernel event must be delivered at level=trace (the default WARN would \
             drop it) — this proves the level argument is honoured; captured={got:?}"
        )
    });
    assert!(
        message.contains("callback probe message"),
        "message text should carry through, got: {message}"
    );
    assert!(
        message.contains("statement_id"),
        "structured field should render into the message, got: {message}"
    );

    // === Step 3: an event emitted INSIDE a (now-disabled) kernel span must
    // still be delivered. ===
    // The bridge's filter is event-only: kernel SPAN callsites cache off
    // (`callsite_interest_for(_, is_event=false)` → never), so the kernel's own
    // `info_span!` / `#[instrument]` spans become no-op disabled spans and pay
    // no span machinery. This must NOT suppress EVENTS emitted while such a span
    // is on the stack — `tracing` evaluates span and event callsites
    // independently, and the kernel logs most of its events from inside
    // instrumented functions (e.g. a `klog::debug!` inside the `#[instrument]`ed
    // `next_batch`). Reproduce that shape: enter a kernel span (disabled by our
    // filter) and emit a kernel event within it; the callback must still get the
    // event. Guards against a filter/`tracing` change silently dropping every
    // in-span kernel log.
    let in_span_marker = "probe-event-inside-disabled-span-c3";
    let kernel_span = tracing::info_span!(
        target: databricks_sql_kernel::KERNEL_LOG_TARGET,
        "kernel.session",
        session_id = "sess-guard"
    );
    {
        let _entered = kernel_span.enter();
        tracing::debug!(target: databricks_sql_kernel::KERNEL_LOG_TARGET, "{}", in_span_marker);
    }
    let got = wait_until(|g| saw_marker(g, in_span_marker));
    assert!(
        saw_marker(&got, in_span_marker),
        "an event emitted inside a disabled kernel span must still be delivered — disabling \
         span callsites must not suppress in-span events; captured={got:?}"
    );

    // The ctx round-trip: the kernel must hand back the exact pointer we
    // registered, verbatim (it is opaque and never dereferenced kernel-side).
    assert_eq!(
        SEEN_CTX.load(Ordering::SeqCst),
        ctx_ptr as usize,
        "callback ctx should round-trip the exact pointer passed to kernel_set_log_callback"
    );

    // Stats accessor: this callback won the subscriber (installed=true) and a
    // healthy delivery drops nothing (the filtered WARN was never enqueued).
    let mut installed = false;
    let mut dropped: u64 = 0;
    unsafe {
        databricks_sql_kernel::c_abi::logging::kernel_logging_stats(&mut installed, &mut dropped)
    };
    assert!(
        installed,
        "kernel_logging_stats should report installed=true after a successful install"
    );
    assert_eq!(dropped, 0, "a healthy delivery should not drop records");

    // === Step 4: a callback that unwinds is counted as a drop and poisons the
    // sink. ===
    // Re-register a callback that always panics (which clears the poison and
    // starts a fresh generation), emit one kernel event, and confirm the bridge
    // (a) contained the unwind — the process is still alive to run these asserts
    // — (b) counted the undelivered record in `dropped` (previously the first
    // unwinding record was silently uncounted), and (c) poisoned the sink so
    // `installed` now reads false.
    let panic_ctx: Box<u64> = Box::new(0xBADF00D);
    let panic_ctx_ptr = Box::into_raw(panic_ctx) as *mut c_void;
    let level_trace2 = std::ffi::CString::new("trace").unwrap();
    let rc =
        unsafe { kernel_set_log_callback(Some(panic_cb), panic_ctx_ptr, level_trace2.as_ptr()) };
    assert_eq!(
        rc,
        KernelStatusCode::Success,
        "re-registering the panicking callback must succeed; got {rc:?}"
    );

    let dropped_before = dropped;
    tracing::error!(target: databricks_sql_kernel::KERNEL_LOG_TARGET, "probe-unwinds-d4");
    // Poll for the sink to be poisoned (delivery attempted, callback unwound).
    let mut installed_after = true;
    let mut dropped_after = dropped_before;
    for _ in 0..100 {
        unsafe {
            databricks_sql_kernel::c_abi::logging::kernel_logging_stats(
                &mut installed_after,
                &mut dropped_after,
            )
        };
        if !installed_after {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        !installed_after,
        "a callback that unwinds must poison the sink so installed reads false"
    );
    assert!(
        dropped_after > dropped_before,
        "the record whose callback unwound must be counted in dropped \
         (before={dropped_before}, after={dropped_after})"
    );

    // === Step 5: re-registering a good callback after poison restores delivery.
    // ===
    // The host's documented recovery path: install a fresh, well-behaved
    // callback. Registration clears the poison and starts a new generation, so
    // `installed` must flip back to true and a NEW event must be delivered again
    // — proving a single misbehaving callback doesn't permanently wedge the
    // bridge for the rest of the process.
    let recover_ctx: Box<u64> = Box::new(0x600D);
    let recover_ctx_ptr = Box::into_raw(recover_ctx) as *mut c_void;
    let level_trace3 = std::ffi::CString::new("trace").unwrap();
    let rc = unsafe {
        kernel_set_log_callback(Some(capture_cb), recover_ctx_ptr, level_trace3.as_ptr())
    };
    assert_eq!(
        rc,
        KernelStatusCode::Success,
        "re-registering a good callback after poison must succeed; got {rc:?}"
    );

    let mut installed_recovered = false;
    let mut dropped_recovered = 0u64;
    unsafe {
        databricks_sql_kernel::c_abi::logging::kernel_logging_stats(
            &mut installed_recovered,
            &mut dropped_recovered,
        )
    };
    assert!(
        installed_recovered,
        "re-registering a good callback must clear the poison and flip installed back to true"
    );

    let recovery_marker = "probe-recovered-e5";
    tracing::error!(target: databricks_sql_kernel::KERNEL_LOG_TARGET, "{}", recovery_marker);
    let got = wait_until(|g| saw_marker(g, recovery_marker));
    assert!(
        saw_marker(&got, recovery_marker),
        "delivery must resume after re-registering a good callback post-poison; captured={got:?}"
    );

    // Reclaim the leaked ctx markers (kept alive across the calls above).
    unsafe {
        drop(Box::from_raw(ctx_ptr as *mut u64));
        drop(Box::from_raw(panic_ctx_ptr as *mut u64));
        drop(Box::from_raw(recover_ctx_ptr as *mut u64));
    }
}

/// `kernel_logging_stats` clears the thread-local last-error on entry like every
/// other C-ABI entry point, so a host that polls this infallible accessor
/// between a failed call and reading the error doesn't observe the prior error
/// through it. Runs on its own test thread (last-error is thread-local), and the
/// null-cb rejection that seeds the error returns BEFORE any install logic, so
/// this test is independent of the delivery test's process-global subscriber.
#[test]
fn kernel_logging_stats_clears_stale_last_error() {
    // Seed a stale error on this thread: a null callback is rejected with
    // InvalidArgument and stores a last error.
    let rc = unsafe { kernel_set_log_callback(None, std::ptr::null_mut(), std::ptr::null()) };
    assert_eq!(
        rc,
        KernelStatusCode::InvalidArgument,
        "a null callback must be rejected and seed a last error"
    );
    let mut seeded = std::mem::MaybeUninit::<KernelError>::uninit();
    assert!(
        unsafe { kernel_get_last_error(seeded.as_mut_ptr()) },
        "precondition: the null-cb rejection must have seeded a last error"
    );

    // The infallible stats accessor must clear the stale error on entry.
    let mut installed = false;
    let mut dropped = 0u64;
    unsafe { kernel_logging_stats(&mut installed, &mut dropped) };

    let mut after = std::mem::MaybeUninit::<KernelError>::uninit();
    assert!(
        !unsafe { kernel_get_last_error(after.as_mut_ptr()) },
        "kernel_logging_stats must clear the stale last error on entry"
    );
}
