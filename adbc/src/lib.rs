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

//! ADBC driver for Databricks backed by the Databricks SQL Kernel.

mod cancellation;
mod config;
mod connection;
mod database;
mod driver;
mod error;
mod reader;
mod runtime;
mod statement;

pub use connection::Connection;
pub use database::Database;
pub use driver::Driver;
pub use statement::{
    AsyncStatementStatus, Statement, StatementCancellationHandle, SubmittedStatement,
};

#[cfg(feature = "ffi")]
unsafe fn initialize_ffi_driver(
    version: std::os::raw::c_int,
    driver: *mut std::os::raw::c_void,
    error: *mut adbc_ffi::FFI_AdbcError,
) -> adbc_core::error::AdbcStatusCode {
    let version = adbc_ffi::check_err!(adbc_core::options::AdbcVersion::try_from(version), error);
    if !matches!(
        version,
        adbc_core::options::AdbcVersion::V100 | adbc_core::options::AdbcVersion::V110
    ) {
        let error_value = adbc_core::error::Error::with_message_and_status(
            format!(
                "Unsupported ADBC version: got={version:?} expected one of {:?}",
                [
                    adbc_core::options::AdbcVersion::V100,
                    adbc_core::options::AdbcVersion::V110,
                ]
            ),
            adbc_core::error::Status::NotImplemented,
        );
        adbc_ffi::check_err!(Err::<(), _>(error_value), error);
    }
    adbc_ffi::check_not_null!(driver, error);

    let mut ffi_driver = <Driver as adbc_ffi::FFIDriver>::ffi_driver();
    // adbc_ffi 0.24 obtains unsynchronized mutable references before calling
    // either cancel method. Advertising them would violate Rust aliasing when
    // cancellation races an operation, before this driver's thread-safe
    // cancellation handles are reached. Null slots select the standard
    // NOT_IMPLEMENTED fallbacks.
    ffi_driver.ConnectionCancel = None;
    ffi_driver.StatementCancel = None;
    let size = match version {
        adbc_core::options::AdbcVersion::V100 => {
            std::mem::offset_of!(adbc_ffi::FFI_AdbcDriver, ErrorGetDetailCount)
        }
        adbc_core::options::AdbcVersion::V110 => std::mem::size_of::<adbc_ffi::FFI_AdbcDriver>(),
        _ => unreachable!("unsupported versions return before driver initialization"),
    };
    unsafe {
        std::ptr::copy_nonoverlapping(
            std::ptr::from_ref(&ffi_driver).cast::<u8>(),
            driver.cast::<u8>(),
            size,
        );
    }
    adbc_core::constants::ADBC_STATUS_OK
}

/// Initialize the Databricks ADBC 1.0 or 1.1 C driver.
///
/// # Safety
///
/// `driver` must point to writable storage for an `FFI_AdbcDriver`. When
/// non-null, `error` must point to writable storage for an `FFI_AdbcError`.
#[cfg(feature = "ffi")]
#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AdbcDatabricksInit(
    version: std::os::raw::c_int,
    driver: *mut std::os::raw::c_void,
    error: *mut adbc_ffi::FFI_AdbcError,
) -> adbc_core::error::AdbcStatusCode {
    unsafe { initialize_ffi_driver(version, driver, error) }
}

/// Conventional driver-manager alias for [`AdbcDatabricksInit`].
///
/// # Safety
///
/// The pointer requirements are identical to [`AdbcDatabricksInit`].
#[cfg(feature = "ffi")]
#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AdbcDriverInit(
    version: std::os::raw::c_int,
    driver: *mut std::os::raw::c_void,
    error: *mut adbc_ffi::FFI_AdbcError,
) -> adbc_core::error::AdbcStatusCode {
    unsafe { AdbcDatabricksInit(version, driver, error) }
}

#[cfg(all(test, feature = "ffi"))]
mod ffi_tests {
    use adbc_core::options::AdbcVersion;

    use super::*;

    #[test]
    fn ffi_cancel_methods_use_safe_not_implemented_fallbacks() {
        for initialize in [AdbcDatabricksInit, AdbcDriverInit] {
            let mut driver = adbc_ffi::FFI_AdbcDriver::default();
            let mut error = adbc_ffi::FFI_AdbcError::default();
            let status = unsafe {
                initialize(
                    AdbcVersion::V110.into(),
                    std::ptr::from_mut(&mut driver).cast(),
                    &mut error,
                )
            };
            assert_eq!(status, adbc_core::constants::ADBC_STATUS_OK);
            assert!(driver.ConnectionCancel.is_none());
            assert!(driver.StatementCancel.is_none());

            let connection_cancel = adbc_ffi::driver_method!(driver, ConnectionCancel);
            let mut connection = adbc_ffi::FFI_AdbcConnection::default();
            let status = unsafe { connection_cancel(&mut connection, &mut error) };
            assert_eq!(status, adbc_core::constants::ADBC_STATUS_NOT_IMPLEMENTED);

            let statement_cancel = adbc_ffi::driver_method!(driver, StatementCancel);
            let mut statement = adbc_ffi::FFI_AdbcStatement::default();
            let status = unsafe { statement_cancel(&mut statement, &mut error) };
            assert_eq!(status, adbc_core::constants::ADBC_STATUS_NOT_IMPLEMENTED);
        }
    }

    #[test]
    fn ffi_v100_initialization_writes_only_the_v100_prefix() {
        const CANARY_SIZE: usize = 32;
        let prefix_size = std::mem::offset_of!(adbc_ffi::FFI_AdbcDriver, ErrorGetDetailCount);
        for initialize in [AdbcDatabricksInit, AdbcDriverInit] {
            let mut storage = vec![0xa5_u8; prefix_size + CANARY_SIZE];
            let mut error = adbc_ffi::FFI_AdbcError::default();
            let status = unsafe {
                initialize(
                    AdbcVersion::V100.into(),
                    storage.as_mut_ptr().cast(),
                    &mut error,
                )
            };
            assert_eq!(status, adbc_core::constants::ADBC_STATUS_OK);
            assert!(storage[..prefix_size].iter().any(|byte| *byte != 0xa5));
            assert!(storage[prefix_size..].iter().all(|byte| *byte == 0xa5));
        }
    }
}
