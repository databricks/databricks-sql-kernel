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

//! Panic-containment regression coverage for callback logging. This lives in a
//! separate test binary because the tracing subscriber is process-global.

use databricks_sql_kernel::c_abi::session::kernel_init_logging_callback;
use databricks_sql_kernel::c_abi::KernelStatusCode;
use databricks_sql_kernel::KERNEL_LOG_TARGET;
use std::ffi::{c_char, c_void, CString};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

static CALLBACK_HITS: AtomicUsize = AtomicUsize::new(0);

struct PanickingDebug;

impl fmt::Debug for PanickingDebug {
    fn fmt(&self, _formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("debug formatting panic probe");
    }
}

unsafe extern "C" fn capture_callback(
    _level: *const c_char,
    _target: *const c_char,
    _message: *const c_char,
    _user_data: *mut c_void,
) {
    CALLBACK_HITS.fetch_add(1, Ordering::SeqCst);
}

#[test]
fn field_debug_panic_is_contained_and_delivery_resets() {
    let level = CString::new("DEBUG").unwrap();

    // SAFETY: the callback is process-lifetime and level is NUL-terminated.
    let status = unsafe {
        kernel_init_logging_callback(level.as_ptr(), Some(capture_callback), std::ptr::null_mut())
    };
    assert_eq!(status, KernelStatusCode::Success);

    let before_panic = CALLBACK_HITS.load(Ordering::SeqCst);
    let emit_result = std::panic::catch_unwind(|| {
        tracing::debug!(
            target: KERNEL_LOG_TARGET,
            panicking = ?PanickingDebug,
            "panic containment probe"
        );
    });
    assert!(
        emit_result.is_ok(),
        "a field Debug panic must not escape callback delivery"
    );
    assert_eq!(
        CALLBACK_HITS.load(Ordering::SeqCst),
        before_panic,
        "an event that fails during formatting must not be delivered"
    );

    tracing::debug!(target: KERNEL_LOG_TARGET, "post-panic reset probe");
    assert_eq!(
        CALLBACK_HITS.load(Ordering::SeqCst),
        before_panic + 1,
        "delivery must resume after the contained panic"
    );
}
