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

//! Reentrancy regression coverage for callback logging. This lives in a
//! separate test binary because the tracing subscriber is process-global.

use databricks_sql_kernel::c_abi::session::kernel_init_logging_callback;
use databricks_sql_kernel::c_abi::KernelStatusCode;
use databricks_sql_kernel::KERNEL_LOG_TARGET;
use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::atomic::{AtomicUsize, Ordering};

static CALLBACK_HITS: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn reentrant_callback(
    _level: *const c_char,
    _target: *const c_char,
    message: *const c_char,
    _user_data: *mut c_void,
) {
    CALLBACK_HITS.fetch_add(1, Ordering::SeqCst);

    // SAFETY: the callback contract guarantees a valid NUL-terminated string
    // for the duration of this call.
    let message = unsafe { CStr::from_ptr(message) }.to_string_lossy();
    if message.contains("outer reentrancy probe") {
        tracing::debug!(
            target: KERNEL_LOG_TARGET,
            "nested reentrancy probe must be dropped"
        );
    }
}

#[test]
fn callback_reemission_is_dropped_without_double_delivery() {
    let level = CString::new("DEBUG").unwrap();

    // SAFETY: the callback is process-lifetime and level is NUL-terminated.
    let status = unsafe {
        kernel_init_logging_callback(
            level.as_ptr(),
            Some(reentrant_callback),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(status, KernelStatusCode::Success);

    let before_reentrancy = CALLBACK_HITS.load(Ordering::SeqCst);
    tracing::debug!(target: KERNEL_LOG_TARGET, "outer reentrancy probe");
    assert_eq!(
        CALLBACK_HITS.load(Ordering::SeqCst),
        before_reentrancy + 1,
        "the nested callback event must not be delivered recursively"
    );

    tracing::debug!(target: KERNEL_LOG_TARGET, "post-reentrancy reset probe");
    assert_eq!(
        CALLBACK_HITS.load(Ordering::SeqCst),
        before_reentrancy + 2,
        "delivery must resume after the callback returns"
    );
}
