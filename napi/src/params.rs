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

//! JS↔napi parameter input types plus a re-export of the kernel codec.
//!
//! The `(sql_type, value_str) -> TypedValue` parsing logic used to live
//! here, duplicated in `pyo3/src/params.rs`. It now lives once in the
//! kernel at `databricks_sql_kernel::statement::params_codec` (re-exported
//! as `databricks_sql_kernel::parse_typed_value`), so both bindings share
//! one codec. As a result `GEOGRAPHY` / `GEOMETRY` are now supported in
//! napi exactly as in pyo3 — the earlier napi-only divergence (geo types
//! falling through to `unsupported parameter SQL type`) is gone.
//!
//! This module re-exports the kernel codec under `crate::params::` so the
//! existing `crate::params::parse_typed_value` callers in `connection.rs`
//! keep resolving unchanged, and it keeps the napi-visible
//! [`TypedValueInput`] struct (referenced by `connection.rs`).
//!
//! Compound types (`ARRAY` / `MAP` / `STRUCT`) and BINARY remain
//! explicitly rejected by the kernel codec — the SEA wire does not accept
//! either as a bound parameter today, surfaced as an `InvalidArgument`
//! error at bind time.

pub(crate) use databricks_sql_kernel::parse_typed_value;

/// JS-visible binding for a single positional parameter.
///
/// Shape mirrors the `TSparkParameter` wire object the Thrift backend
/// already emits via `DBSQLParameter.toSparkParameter()` — `type` is the
/// canonical Databricks SQL type name (`"INT"`, `"STRING"`,
/// `"DECIMAL(10,2)"`, ...), `value` is the string-encoded literal or
/// `None` for SQL NULL.
///
/// Why a string for `value` instead of a tagged JS union: round-tripping
/// arbitrary JS values across the FFI requires either (a) a custom
/// napi `FromNapiValue` per arm, or (b) a `serde_json::Value`-style
/// dynamic dispatch on the Rust side. The Node-driver adapter already
/// stringifies before calling the binding (see `DBSQLParameter` and the
/// existing pyo3 wrapper), so the string-in / string-parsed contract
/// adds no JS-side complexity and keeps the kernel-side validation in
/// one place.
#[allow(
    dead_code,
    reason = "exported via napi macro codegen; not called from Rust"
)]
#[napi(object)]
pub struct TypedValueInput {
    /// Canonical Databricks SQL type name. Case-insensitive for the
    /// simple variants; for DECIMAL the parenthesised form
    /// (`"DECIMAL(10,2)"`) is required so the kernel can extract
    /// precision/scale.
    pub sql_type: String,
    /// String-encoded value. `None` always produces `TypedValue::Null`
    /// regardless of `sql_type` — matches the connector's
    /// `VoidParameter` shape and the pyo3 binding's contract.
    pub value: Option<String>,
}

/// A pre-marshalled SEA parameter that preserves `sql_type` verbatim.
/// Omit `name` for positional markers; the kernel assigns their ordinals.
#[napi(object)]
pub struct RawParameterInput {
    /// Named marker name. Omit for a positional marker.
    pub name: Option<String>,
    /// Databricks SQL type name sent verbatim to SEA.
    pub sql_type: String,
    /// String-encoded value. `None` represents SQL NULL.
    pub value: Option<String>,
}
