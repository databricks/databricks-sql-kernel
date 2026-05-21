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

//! Shared metadata utilities used by the v0 `Metadata` surface.
//!
//! Protocol-neutral building blocks:
//! - `SqlCommandBuilder`: Builds SHOW SQL commands for metadata queries.
//! - `schemas`, `static_data`: kernel-synthesised result schemas for
//!   `type_info` / `table_types`.
//! - `type_mapping`: Maps Databricks types to Arrow/XDBC types.

pub(crate) mod schemas;
pub mod sql;
pub(crate) mod static_data;
pub(crate) mod thrift_shape;
pub mod type_mapping;
