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

use adbc_core::options::AdbcVersion;
use adbc_core::Driver as _;
use adbc_driver_manager::ManagedDriver;
use std::path::PathBuf;

const ENTRYPOINTS: [&[u8]; 2] = [b"AdbcDatabricksInit", b"AdbcDriverInit"];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let library = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: ffi_smoke <absolute-path-to-driver-library>")?;

    if !library.is_absolute() {
        return Err("driver library path must be absolute".into());
    }

    let mut evidence = String::new();
    for entrypoint in ENTRYPOINTS {
        let entrypoint_name = std::str::from_utf8(entrypoint)?;
        let mut ffi_driver = adbc_ffi::FFI_AdbcDriver::default();
        let mut ffi_error = adbc_ffi::FFI_AdbcError::default();
        let status = match entrypoint_name {
            "AdbcDatabricksInit" => unsafe {
                databricks_adbc::AdbcDatabricksInit(
                    AdbcVersion::V110.into(),
                    std::ptr::from_mut(&mut ffi_driver).cast(),
                    &mut ffi_error,
                )
            },
            "AdbcDriverInit" => unsafe {
                databricks_adbc::AdbcDriverInit(
                    AdbcVersion::V110.into(),
                    std::ptr::from_mut(&mut ffi_driver).cast(),
                    &mut ffi_error,
                )
            },
            _ => unreachable!("fixed entrypoint list"),
        };
        if status != adbc_core::constants::ADBC_STATUS_OK {
            return Err(format!("{entrypoint_name} initialization returned {status}").into());
        }
        if ffi_driver.ConnectionCancel.is_some() || ffi_driver.StatementCancel.is_some() {
            return Err(format!("{entrypoint_name} unsafely advertised cancellation").into());
        }

        let connection_cancel = adbc_ffi::driver_method!(ffi_driver, ConnectionCancel);
        let mut uninitialized_connection = adbc_ffi::FFI_AdbcConnection::default();
        let cancel_status =
            unsafe { connection_cancel(&mut uninitialized_connection, &mut ffi_error) };
        if cancel_status != adbc_core::constants::ADBC_STATUS_NOT_IMPLEMENTED {
            return Err(format!(
                "{entrypoint_name} ConnectionCancel fallback returned {cancel_status}"
            )
            .into());
        }

        let statement_cancel = adbc_ffi::driver_method!(ffi_driver, StatementCancel);
        let mut uninitialized_statement = adbc_ffi::FFI_AdbcStatement::default();
        let cancel_status =
            unsafe { statement_cancel(&mut uninitialized_statement, &mut ffi_error) };
        if cancel_status != adbc_core::constants::ADBC_STATUS_NOT_IMPLEMENTED {
            return Err(format!(
                "{entrypoint_name} StatementCancel fallback returned {cancel_status}"
            )
            .into());
        }

        for (version, version_name) in [(AdbcVersion::V100, "1.0"), (AdbcVersion::V110, "1.1")] {
            let mut driver =
                ManagedDriver::load_dynamic_from_filename(&library, Some(entrypoint), version)?;
            let _database = driver.new_database()?;
            let result = format!(
                "{{\"entrypoint\":\"{entrypoint_name}\",\"adbc_version\":\"{version_name}\",\"driver_manager_version\":\"0.24.0\",\"database_allocated\":true}}"
            );
            println!("{result}");
            evidence.push_str(&result);
            evidence.push('\n');
        }
    }

    if let Some(output) = std::env::args_os().nth(2) {
        std::fs::write(output, evidence)?;
    }

    Ok(())
}
