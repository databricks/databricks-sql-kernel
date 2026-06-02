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

//! Thin re-export of the kernel parameter codec.
//!
//! The `(sql_type, value_str) -> TypedValue` parsing logic used to live
//! here, duplicated in `napi/src/params.rs`. It now lives once in the
//! kernel at `databricks_sql_kernel::statement::params_codec` (re-exported
//! as `databricks_sql_kernel::parse_typed_value`), so both bindings share
//! a single source of truth and can never diverge in what the kernel sees
//! on the wire. This module just re-exports it under `crate::params::` so
//! the existing `crate::params::parse_typed_value` callers in
//! `statement.rs` keep resolving unchanged.

pub(crate) use databricks_sql_kernel::parse_typed_value;
