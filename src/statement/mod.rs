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

//! Shared statement-level utilities used by all kernel surfaces.
//!
//! The kernel-spec `Statement` type lives at `crate::statement` (top-level
//! file). This module re-exports the protocol-neutral parameter helpers so
//! both the new `Statement` and the legacy ADBC `Statement` can share them.

pub mod params;
