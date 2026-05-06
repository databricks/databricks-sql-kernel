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

//! Custom C ABI for the v0 Kernel — for ODBC and Go consumers that cannot
//! link the Rust crate directly.
//!
//! Per the spec (`docs/spec-v0.md`, "Architecture"), this layer is a thin
//! shim over the async kernel core: it owns a process-wide tokio runtime,
//! marshals C strings / structs into `SessionConfig`, and exposes opaque
//! handles for `Session`, `Statement`, and `ResultStream`.
//!
//! Populated incrementally by PRs 7a–7d in the staged refactor. The header
//! at `include/databricks_kernel.h` is the C-side contract.
//!
//! Distinct from the legacy `crate::adbc::ffi` module, which exposes the
//! ADBC-shaped metadata-FFI and is gated on `legacy-adbc`.

#![allow(dead_code)]
