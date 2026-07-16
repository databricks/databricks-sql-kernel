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
//! bridge. Registers a C callback, emits a kernel `tracing` event on the
//! contract target, and asserts the callback received it with the message and
//! target intact.
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

use databricks_sql_kernel::c_abi::logging::kernel_set_log_callback;
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

#[test]
fn log_callback_receives_kernel_event_end_to_end() {
    // Drive the threshold through the explicit `level` argument (the one-knob
    // path a host uses) rather than RUST_LOG, so the test proves the level
    // param actually gates delivery. "trace" lets every kernel event through.
    // Register a real (non-null) ctx so we can assert the round-trip returns the
    // exact pointer we passed — a stand-in for the Go cgo.Handle token.
    let ctx_marker: Box<u64> = Box::new(0xC0FFEE);
    let ctx_ptr = Box::into_raw(ctx_marker) as *mut c_void;
    let level = std::ffi::CString::new("trace").unwrap();
    let rc = unsafe { kernel_set_log_callback(Some(capture_cb), ctx_ptr, level.as_ptr()) };
    // Success is expected in this fresh binary (nothing else installed a global
    // subscriber). If some future test in THIS binary installs one first, the
    // bridge would report Internal — accept both so the test pins delivery, not
    // install ordering.
    assert!(
        rc == KernelStatusCode::Success || rc == KernelStatusCode::Internal,
        "kernel_set_log_callback returned an unexpected status: {rc:?}"
    );
    if rc != KernelStatusCode::Success {
        // A global subscriber was already installed by another test in this
        // binary; delivery can't be asserted. There is only one test here today,
        // so this branch is defensive.
        return;
    }

    // Emit an event on the kernel's contract target with a message + a field.
    // `klog::warn!` is pub(crate); from this external test binary we emit the
    // exact `tracing` event it expands to (same target + event field).
    let stmt = "stmt-cb-probe-9c1";
    tracing::warn!(
        target: databricks_sql_kernel::KERNEL_LOG_TARGET,
        statement_id = stmt,
        "callback probe message"
    );

    // Delivery is via the bridge's drain thread; poll briefly for it to land.
    let mut got = Vec::new();
    for _ in 0..100 {
        got = CAPTURED.lock().unwrap().clone();
        if got
            .iter()
            .any(|(_, t, m)| t == databricks_sql_kernel::KERNEL_LOG_TARGET && m.contains(stmt))
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    let hit = got
        .iter()
        .find(|(_, t, m)| t == databricks_sql_kernel::KERNEL_LOG_TARGET && m.contains(stmt));
    let (_level, _target, message) = hit.unwrap_or_else(|| {
        panic!("the log callback did not receive the kernel event; captured={got:?}")
    });
    assert!(
        message.contains("callback probe message"),
        "message text should carry through, got: {message}"
    );
    assert!(
        message.contains("statement_id"),
        "structured field should render into the message, got: {message}"
    );

    // The ctx round-trip: the kernel must hand back the exact pointer we
    // registered, verbatim (it is opaque and never dereferenced kernel-side).
    assert_eq!(
        SEEN_CTX.load(Ordering::SeqCst),
        ctx_ptr as usize,
        "callback ctx should round-trip the exact pointer passed to kernel_set_log_callback"
    );

    // Stats accessor: this callback won the subscriber (installed=true) and a
    // healthy delivery drops nothing.
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

    // Reclaim the leaked ctx marker (kept alive across the call above).
    unsafe { drop(Box::from_raw(ctx_ptr as *mut u64)) };
}
