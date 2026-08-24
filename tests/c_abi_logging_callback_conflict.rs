// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! First-call-wins identity for the callback log sink: a second, *different*
//! callback registration must report that it was NOT installed (`Internal`),
//! not a misleading `Success` that leaves a host believing a dropped callback
//! is live — and only the winning callback receives records. This is its own
//! test binary because the kernel tracing subscriber is process-wide and
//! installed exactly once per process.

use databricks_sql_kernel::c_abi::session::kernel_init_logging_callback;
use databricks_sql_kernel::c_abi::KernelStatusCode;
use databricks_sql_kernel::KERNEL_LOG_TARGET;
use std::ffi::{c_char, c_void, CString};
use std::sync::atomic::{AtomicUsize, Ordering};

static A_HITS: AtomicUsize = AtomicUsize::new(0);
static B_HITS: AtomicUsize = AtomicUsize::new(0);

// Two callbacks with distinct bodies (different counters) and distinct
// user_data, so their identities differ even if the compiler folds identical
// function bodies — the registration is keyed on (callback, user_data).
unsafe extern "C" fn callback_a(
    _level: *const c_char,
    _target: *const c_char,
    _message: *const c_char,
    _user_data: *mut c_void,
) {
    A_HITS.fetch_add(1, Ordering::SeqCst);
}

unsafe extern "C" fn callback_b(
    _level: *const c_char,
    _target: *const c_char,
    _message: *const c_char,
    _user_data: *mut c_void,
) {
    B_HITS.fetch_add(1, Ordering::SeqCst);
}

#[test]
fn second_distinct_callback_registration_is_rejected() {
    let level = CString::new("DEBUG").unwrap();
    let ud_a = 0xA_usize as *mut c_void;
    let ud_b = 0xB_usize as *mut c_void;

    // A wins the process-global, first-call-wins install.
    // SAFETY: `callback_a`/`ud_a` remain valid for the process lifetime and
    // `level` is a valid NUL-terminated string.
    let status_a = unsafe { kernel_init_logging_callback(level.as_ptr(), Some(callback_a), ud_a) };
    assert_eq!(
        status_a,
        KernelStatusCode::Success,
        "the first callback must install"
    );

    // Re-registering the *identical* callback + user_data is idempotent: that
    // identity IS the installed sink, so it still succeeds.
    // SAFETY: same contract as above.
    let status_a_again =
        unsafe { kernel_init_logging_callback(level.as_ptr(), Some(callback_a), ud_a) };
    assert_eq!(
        status_a_again,
        KernelStatusCode::Success,
        "re-registering the identical callback must stay Success"
    );

    // B is a *different* callback (and different user_data). It loses to the
    // first-call-wins install and must be told so — not a misleading Success.
    // SAFETY: same contract as above.
    let status_b = unsafe { kernel_init_logging_callback(level.as_ptr(), Some(callback_b), ud_b) };
    assert_eq!(
        status_b,
        KernelStatusCode::Internal,
        "a different second callback must report it was not installed"
    );

    // Delivery goes only to the winner: A receives the event, B never does.
    tracing::debug!(target: KERNEL_LOG_TARGET, "callback identity probe");
    assert!(
        A_HITS.load(Ordering::SeqCst) > 0,
        "the winning callback must receive records"
    );
    assert_eq!(
        B_HITS.load(Ordering::SeqCst),
        0,
        "the rejected callback must receive nothing"
    );
}
