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

//! Typed configuration for the v0 Kernel API.
//!
//! Mirrors the "Public Rust API (v0) → Session" section of the spec
//! (`docs/spec-v0.md`): `SessionConfig` and the per-concern sub-config groups
//! (`ConnectionConfig`, `AuthConfig`, `HttpConfig`, `ProxyConfig`, `TlsConfig`,
//! `CloudFetchConfig`, `LoggingConfig`, `TelemetryConfig`, `TransportMode`)
//! plus the server-bound `session_conf` passthrough map.
//!
//! Populated incrementally by PR 2 in the staged refactor; this file is a
//! placeholder to anchor the module path so subsequent PRs are pure
//! additions.

#![allow(dead_code)]
