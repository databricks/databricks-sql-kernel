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

use adbc_core::error::{Error, Status};
use databricks_sql_kernel as kernel;

pub(crate) fn adbc_error(message: impl Into<String>, status: Status) -> Error {
    Error::with_message_and_status(message, status)
}

pub(crate) fn unsupported(operation: &str) -> Error {
    adbc_error(
        format!("{operation} is not implemented by this driver"),
        Status::NotImplemented,
    )
}

pub(crate) fn option_not_found(kind: &str, key: &str) -> Error {
    adbc_error(
        format!("{kind} option {key} was not found"),
        Status::NotFound,
    )
}

pub(crate) fn map_kernel_error(source: kernel::Error) -> Error {
    let status = match source.code {
        kernel::ErrorCode::InvalidArgument => Status::InvalidArguments,
        kernel::ErrorCode::Unauthenticated => Status::Unauthenticated,
        kernel::ErrorCode::PermissionDenied => Status::Unauthorized,
        kernel::ErrorCode::NotFound => Status::NotFound,
        kernel::ErrorCode::ResourceExhausted
        | kernel::ErrorCode::Unavailable
        | kernel::ErrorCode::NetworkError => Status::IO,
        kernel::ErrorCode::Timeout => Status::Timeout,
        kernel::ErrorCode::Cancelled => Status::Cancelled,
        kernel::ErrorCode::DataLoss => Status::InvalidData,
        kernel::ErrorCode::InvalidStatementHandle => Status::InvalidState,
        kernel::ErrorCode::Internal => Status::Internal,
        kernel::ErrorCode::SqlError => sql_status(source.sql_state.as_deref()),
        _ => Status::Unknown,
    };
    let mut mapped = adbc_error(source.message, status);
    mapped.vendor_code = source.vendor_code.unwrap_or_default();
    if let Some(sqlstate) = source.sql_state.filter(|value| value.len() == 5) {
        for (target, source) in mapped.sqlstate.iter_mut().zip(sqlstate.bytes()) {
            *target = source as std::ffi::c_char;
        }
    }
    mapped.details = source
        .error_code
        .map(|code| vec![("databricks.error_code".to_string(), code.into_bytes())]);
    mapped
}

fn sql_status(sqlstate: Option<&str>) -> Status {
    match sqlstate {
        Some(state) if state.starts_with("23") => Status::Integrity,
        Some("42P01" | "42703" | "42704" | "42S02") => Status::NotFound,
        Some("42P07" | "42710" | "42S01") => Status::AlreadyExists,
        Some(state) if state.starts_with("22") || state.starts_with("42") => Status::InvalidData,
        Some(state) if state.starts_with("25") => Status::InvalidState,
        Some(state) if state.starts_with("28") => Status::Unauthenticated,
        _ => Status::InvalidData,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_operations_use_not_implemented() {
        assert_eq!(
            unsupported("partitioned execution").status,
            Status::NotImplemented
        );
    }

    #[test]
    fn unknown_option_getters_use_not_found() {
        assert_eq!(
            option_not_found("statement string", "unknown").status,
            Status::NotFound
        );
    }

    #[test]
    fn kernel_error_preserves_sqlstate_and_vendor_code() {
        let mapped = map_kernel_error(
            kernel::Error::sql_error()
                .message("bad query")
                .with_sql_state("42000")
                .with_vendor_code(42),
        );
        assert_eq!(mapped.status, Status::InvalidData);
        assert_eq!(mapped.vendor_code, 42);
        assert_eq!(
            mapped.sqlstate,
            (*b"42000").map(|byte| byte as std::ffi::c_char)
        );
    }

    #[test]
    fn kernel_error_maps_handle_and_sqlstate_categories() {
        let handle = map_kernel_error(kernel::Error::invalid_statement_handle());
        assert_eq!(handle.status, Status::InvalidState);

        for (sqlstate, expected) in [
            ("23505", Status::Integrity),
            ("42P01", Status::NotFound),
            ("42703", Status::NotFound),
            ("42P07", Status::AlreadyExists),
            ("42601", Status::InvalidData),
            ("25000", Status::InvalidState),
            ("28000", Status::Unauthenticated),
        ] {
            let mapped = map_kernel_error(
                kernel::Error::sql_error()
                    .message("server error")
                    .with_sql_state(sqlstate),
            );
            assert_eq!(mapped.status, expected, "SQLSTATE {sqlstate}");
        }
    }
}
