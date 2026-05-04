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

//! Legacy ADBC surface — quarantined.
//!
//! This module holds the original ADBC trait implementations seeded from the
//! adbc-drivers/databricks driver. It is gated behind the `legacy-adbc`
//! cargo feature and will be removed once the v0 Kernel API at the crate
//! root is the sole supported surface (see `docs/spec-v0.md` and the staged
//! refactor plan).
//!
//! New code should not import from `crate::adbc::*`.

pub mod connection;
pub mod database;
pub mod driver;
pub mod metadata_builder;
pub mod statement;

#[cfg(feature = "metadata-ffi")]
pub mod ffi;
