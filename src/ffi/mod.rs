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

//! Custom C ABI for the v0 Kernel — for ODBC and Go consumers that
//! cannot link the Rust crate directly.
//!
//! Per `docs/spec-v0.md` "Architecture", this layer is a thin shim over
//! the async kernel core: it owns a process-wide tokio runtime, marshals
//! C strings / structs into `SessionConfig`, and exposes opaque handles
//! for `Session`, `Statement`, `ExecutedStatement`,
//! `ExecutedAsyncStatement`, and `ResultStream`.
//!
//! Distinct from the legacy `crate::adbc::ffi` module (ADBC-shaped
//! metadata-FFI, gated on `legacy-adbc`).

#![allow(dead_code, unused_variables)]
// Skeleton: each function will get its `# Safety` doc when its body
// lands in PRs 7a–7d. Suppress the lint at the module root for now so
// reviewers can read the signatures without 31 lint markers.
#![allow(clippy::missing_safety_doc)]

pub mod error;
pub mod metadata;
pub mod result;
pub mod session;
pub mod statement;

/// Status enum returned by every C entry point.
///
/// Variants parallel [`crate::kernel_error::ErrorCode`] one-to-one. The
/// [`From<ErrorCode>`] conversion below uses an exhaustive match (no
/// `_ =>` arm) so adding a variant to `ErrorCode` is a compile error
/// here until `KernelStatusCode` grows the matching variant. That
/// catches FFI/Rust drift mechanically.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelStatusCode {
    Success = 0,
    InvalidArgument = 1,
    Unauthenticated = 2,
    PermissionDenied = 3,
    NotFound = 4,
    ResourceExhausted = 5,
    Unavailable = 6,
    Timeout = 7,
    Cancelled = 8,
    DataLoss = 9,
    Internal = 10,
    InvalidStatementHandle = 11,
    NetworkError = 12,
    SqlError = 13,
}

impl From<crate::kernel_error::ErrorCode> for KernelStatusCode {
    fn from(code: crate::kernel_error::ErrorCode) -> Self {
        use crate::kernel_error::ErrorCode;
        // Exhaustive match — no `_ =>` arm. Adding a variant to
        // `ErrorCode` is a compile error here, forcing the FFI to
        // declare the corresponding `KernelStatusCode` variant.
        match code {
            ErrorCode::InvalidArgument => Self::InvalidArgument,
            ErrorCode::Unauthenticated => Self::Unauthenticated,
            ErrorCode::PermissionDenied => Self::PermissionDenied,
            ErrorCode::NotFound => Self::NotFound,
            ErrorCode::ResourceExhausted => Self::ResourceExhausted,
            ErrorCode::Unavailable => Self::Unavailable,
            ErrorCode::Timeout => Self::Timeout,
            ErrorCode::Cancelled => Self::Cancelled,
            ErrorCode::DataLoss => Self::DataLoss,
            ErrorCode::Internal => Self::Internal,
            ErrorCode::InvalidStatementHandle => Self::InvalidStatementHandle,
            ErrorCode::NetworkError => Self::NetworkError,
            ErrorCode::SqlError => Self::SqlError,
        }
    }
}
