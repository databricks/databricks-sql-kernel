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

use databricks_sql_kernel::c_abi::session::kernel_init_logging_callback;
use databricks_sql_kernel::c_abi::KernelStatusCode;
use databricks_sql_kernel::KERNEL_LOG_TARGET;
use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::Mutex;

#[derive(Debug)]
struct Record {
    level: String,
    target: String,
    message: String,
    user_data: usize,
}

static RECORDS: Mutex<Vec<Record>> = Mutex::new(Vec::new());

unsafe extern "C" fn capture_log(
    level: *const c_char,
    target: *const c_char,
    message: *const c_char,
    user_data: *mut c_void,
) {
    // SAFETY: the callback contract guarantees non-null, NUL-terminated
    // strings that remain valid for this invocation.
    let level = unsafe { CStr::from_ptr(level) }
        .to_string_lossy()
        .into_owned();
    // SAFETY: same callback contract as above.
    let target = unsafe { CStr::from_ptr(target) }
        .to_string_lossy()
        .into_owned();
    // SAFETY: same callback contract as above.
    let message = unsafe { CStr::from_ptr(message) }
        .to_string_lossy()
        .into_owned();
    RECORDS.lock().unwrap().push(Record {
        level,
        target,
        message,
        user_data: user_data as usize,
    });
}

#[test]
fn callback_receives_kernel_log_record() {
    let level = CString::new("DEBUG").unwrap();
    let user_data = 0x1234usize as *mut c_void;

    // SAFETY: the callback and user-data sentinel remain valid for the process
    // lifetime, and `level` is a valid NUL-terminated string.
    let status =
        unsafe { kernel_init_logging_callback(level.as_ptr(), Some(capture_log), user_data) };
    assert_eq!(status, KernelStatusCode::Success);

    tracing::debug!(
        target: KERNEL_LOG_TARGET,
        answer = 42,
        "callback bridge smoke"
    );

    let records = RECORDS.lock().unwrap();
    let record = records
        .iter()
        .find(|record| record.message.contains("callback bridge smoke"))
        .expect("kernel tracing record must reach the C callback");
    assert_eq!(record.level, "debug");
    assert_eq!(record.target, KERNEL_LOG_TARGET);
    assert!(record.message.contains("answer=42"));
    assert_eq!(record.user_data, 0x1234);
}
