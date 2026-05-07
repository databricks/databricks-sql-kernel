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

//! Statement-level types and utilities.
//!
//! The v0 Kernel spec four-type decomposition lives here as submodules:
//! - [`spec::StatementSpec`] — plain data describing what to execute.
//! - [`mutable::Statement`] — lifecycle owner returned by `Session::statement()`.
//! - [`executed::ExecutedStatement`] — sync-consumption executed handle.
//! - [`executed_async::ExecutedAsyncStatement`] — async-consumption executed handle.
//! - [`handle::ExecutedStatementHandle`] — shared trait across both executed types.
//!
//! `params` holds the protocol-neutral parameter conversion helpers shared
//! by the new `Statement` and the legacy ADBC `Statement`.
//!
//! The legacy ADBC `Statement` (gated on `legacy-adbc`) lives at
//! `crate::adbc::statement`.

pub mod executed;
pub mod executed_async;
pub mod handle;
pub mod mutable;
pub mod params;
pub mod spec;

pub(crate) mod validity;

// Re-exports so wrappers can write `crate::statement::Statement` instead
// of the deeper submodule paths. The crate-root re-exports in `lib.rs`
// also resolve here, so both paths work.
pub use executed::ExecutedStatement;
pub use executed_async::{ExecutedAsyncStatement, StatementStatus};
pub use handle::ExecutedStatementHandle;
pub use mutable::Statement;
pub use spec::{ResultFormat, StatementSpec, TypedValue};
