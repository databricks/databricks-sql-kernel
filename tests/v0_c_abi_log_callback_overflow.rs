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

//! Drop-on-full test for the `kernel_set_log_callback` bridge's bounded channel.
//!
//! The delivery test (`v0_c_abi_log_callback.rs`) covers the healthy path
//! (`dropped == 0`) and the poison path (a record lost to an unwinding
//! callback). It does NOT exercise the headline "logs never back-pressure a
//! kernel hot path" behavior — the `try_send(...).is_err()` overflow branch in
//! `KernelLogLayer::on_event`. This test does: it WEDGES the single drain thread
//! inside a blocking callback so records can't be consumed, floods the bounded
//! channel (capacity 4096) with far more than that many kernel events, and
//! asserts `kernel_logging_stats` reports `dropped > 0` while the emitting
//! thread stays responsive (never blocks). A regression that swapped the
//! non-blocking `try_send` for a blocking `send` would stall the emitter here
//! instead of dropping — which this test would catch as a hang.
//!
//! Its own integration-test binary (like the delivery test): it installs the
//! process-global `tracing` subscriber and permanently wedges the drain thread,
//! so it must not share a process with tests that assert a healthy sink.

use std::os::raw::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use databricks_sql_kernel::c_abi::logging::{kernel_logging_stats, kernel_set_log_callback};
use databricks_sql_kernel::c_abi::KernelStatusCode;

// The channel capacity in `src/c_abi/logging.rs`. Kept in sync by construction:
// we emit comfortably more than this so the overflow branch is guaranteed.
const CHANNEL_CAPACITY: usize = 4096;

// Set by the callback the instant it is first entered — signals that the drain
// thread has pulled one record and is now blocked inside the callback, so any
// further emits must pile onto (and overflow) the bounded channel.
static WEDGED: AtomicBool = AtomicBool::new(false);
// Cleared to release the wedged callback so the drain thread can finish and exit
// cleanly at end of test.
static RELEASE: AtomicBool = AtomicBool::new(false);
static DELIVERED: AtomicUsize = AtomicUsize::new(0);

/// A callback that blocks the drain thread until the test releases it. The first
/// (and, while wedged, only) delivered record parks here; every later buffered
/// record runs fast once released.
extern "C-unwind" fn wedging_cb(
    _ctx: *mut c_void,
    _level: c_int,
    _target: *const c_char,
    _message: *const c_char,
) {
    WEDGED.store(true, Ordering::SeqCst);
    DELIVERED.fetch_add(1, Ordering::SeqCst);
    // Park the drain thread here so the bounded channel fills. Bounded spin so a
    // bug in the release path can't hang CI forever.
    let start = Instant::now();
    while !RELEASE.load(Ordering::SeqCst) && start.elapsed() < Duration::from_secs(30) {
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn drop_on_full_counts_overflow_without_blocking_the_emitter() {
    // Fresh single-test binary: nothing else has claimed the global subscriber,
    // so install MUST succeed. Install at trace so the ERROR flood below clears
    // the threshold and every event reaches the channel.
    let level = std::ffi::CString::new("trace").unwrap();
    let rc =
        unsafe { kernel_set_log_callback(Some(wedging_cb), std::ptr::null_mut(), level.as_ptr()) };
    assert_eq!(
        rc,
        KernelStatusCode::Success,
        "kernel_set_log_callback must succeed on first install in a fresh binary; got {rc:?}"
    );

    // Emit one event and wait for the drain thread to pull it and wedge inside
    // the callback. Once WEDGED, exactly one record has been consumed and the
    // drain thread is blocked — so subsequent emits can only fill the channel.
    tracing::error!(target: databricks_sql_kernel::KERNEL_LOG_TARGET, "wedge-primer");
    let mut wedged = false;
    for _ in 0..500 {
        if WEDGED.load(Ordering::SeqCst) {
            wedged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        wedged,
        "the drain thread should have entered the blocking callback (wedged)"
    );

    // Flood well past the buffer. With the drain thread wedged, at most
    // CHANNEL_CAPACITY records can buffer; the rest hit the try_send-full branch
    // and are counted in `dropped`. Time the loop: if try_send ever became a
    // blocking send, this would stall for ~30s (the callback's park) instead of
    // returning promptly — so a slow loop is itself a failure signal.
    let flood = CHANNEL_CAPACITY * 2 + 500;
    let start = Instant::now();
    for i in 0..flood {
        tracing::error!(target: databricks_sql_kernel::KERNEL_LOG_TARGET, "flood-{}", i);
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "emitting {flood} events must not block the caller (drop-on-full, not back-pressure); \
         took {elapsed:?} — a blocking send would stall on the wedged drain thread"
    );

    // The drops happen synchronously in on_event on this thread during the
    // flood, so `dropped` is already positive; poll briefly only for robustness.
    let mut installed = false;
    let mut dropped: u64 = 0;
    for _ in 0..50 {
        unsafe { kernel_logging_stats(&mut installed, &mut dropped) };
        if dropped > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        dropped > 0,
        "flooding a wedged bridge past its {CHANNEL_CAPACITY}-record channel must drop records \
         (dropped={dropped}); the overflow branch was not exercised"
    );
    assert!(
        installed,
        "the sink is still installed (a full channel drops records, it does not poison the sink)"
    );

    // Release the wedged callback so the drain thread drains the buffer and can
    // exit cleanly; the process would reap it regardless, but this keeps the
    // teardown tidy.
    RELEASE.store(true, Ordering::SeqCst);
}
