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

//! Typed error model for the v0 Kernel API.
//!
//! Mirrors the "Errors" section of the spec (`docs/spec-v0.md`): a typed
//! `Error` struct (code, message, sql_state, vendor_code, http_status,
//! retryable, query_id, cause) and a 13-variant `ErrorCode` enum.
//!
//! Lives at `crate::kernel_error` during the migration to avoid colliding
//! with `crate::error` (the driverbase-backed legacy error). The final
//! removal PR renames this file to `error.rs` and deletes the old one.
//!
//! Populated incrementally by PR 2 in the staged refactor.

#![allow(dead_code)]
