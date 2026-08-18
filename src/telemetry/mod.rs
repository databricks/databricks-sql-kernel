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

//! Telemetry model, collection, and export support.
//!
//! These types mirror the SQL driver telemetry envelope used by the ADBC and
//! JDBC drivers. The module is split by responsibility so later phases can add
//! statement, result-stream, metadata, and production exporter code without
//! growing a single catch-all file.

#![allow(dead_code)]

mod client;
mod exporter;
mod models;
mod runtime;

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
pub(crate) use exporter::{TelemetryExportError, TelemetryExportResult, TelemetryExporter};
pub use models::DriverSystemConfiguration;
pub(crate) use models::OperationType;
#[cfg(test)]
pub(crate) use models::TelemetryRequest;
pub(crate) use runtime::TelemetryRuntime;
