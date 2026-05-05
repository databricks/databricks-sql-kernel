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

//! Session FFI — open / close / new_statement.
//!
//! Transaction methods (autocommit / commit / rollback) are deferred to
//! v0.1 per `docs/spec-v0.md` addendum A5.

#![allow(dead_code, unused_variables)]

use super::KernelStatusCode;

/// Opaque session handle. Boxed `Session` on the Rust side.
#[repr(C)]
pub struct kernel_session_t {
    _private: [u8; 0],
}

/// C-side session config. Opaque `#[non_exhaustive]` so the layout is
/// stable for forward declarations even though PR 7a populates the
/// fields. Same skeleton property as `kernel_session_t`: C consumers
/// can declare a `KernelSessionConfig*` against this header today;
/// constructors / setters land in PR 7a (`kernel_session_config_new`,
/// `kernel_session_config_set_host`, etc.).
#[repr(C)]
#[non_exhaustive]
pub struct KernelSessionConfig {
    _private: [u8; 0],
}

/// Open a session from a `repr(C)` config struct.
///
/// PR 7a finalizes the config struct fields and the
/// `kernel_session_config_*` constructor surface. On success, `*out`
/// holds an owned handle the caller releases with `kernel_session_close`.
///
/// # Safety
/// `config` must point to a valid `KernelSessionConfig` produced by
/// `kernel_session_config_new`; `out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_open(
    config: *const KernelSessionConfig,
    out: *mut *mut kernel_session_t,
) -> KernelStatusCode {
    unimplemented!("PR 7a: marshal config, block_on Session::open")
}

/// Close a session. Releases the underlying Rust `Session`.
///
/// # Safety
///
/// `session` must have been returned by `kernel_session_open` and not
/// previously closed. After this call, `session` is invalid; passing
/// it to any other `kernel_*` function is UB.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_close(session: *mut kernel_session_t) -> KernelStatusCode {
    unimplemented!("PR 7a: drop the boxed Session")
}

/// Construct a new mutable statement bound to this session. Returns a
/// borrowed pointer; the statement's lifetime is bound to the session.
///
/// Caller releases with `kernel_statement_close`.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_new_statement(
    session: *mut kernel_session_t,
    out: *mut *mut super::statement::kernel_statement_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b")
}

// Deferred to v0.1 (see addendum A5):
//
//   pub unsafe extern "C" fn kernel_session_set_autocommit(...) -> KernelStatusCode;
//   pub unsafe extern "C" fn kernel_session_commit(...) -> KernelStatusCode;
//   pub unsafe extern "C" fn kernel_session_rollback(...) -> KernelStatusCode;
