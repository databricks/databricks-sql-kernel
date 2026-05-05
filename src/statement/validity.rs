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

//! `ValidityFlag` — shared validity bit between a `Statement` and the
//! current executed handle it produced.
//!
//! See `docs/spec-v0.md` addendum A1, "Validity flag for zombie
//! handles". Concentrates the `Arc<AtomicBool>` plus its
//! Acquire/Release ordering rules in one place so callers use
//! `flag.invalidate()` / `flag.check()?` and never see the raw atomic
//! ops. PR 5 bodies will land against this type without needing to
//! re-derive the ordering at each call site.

#![allow(dead_code)]

use crate::kernel_error::{Error, ErrorCode, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Cheaply-cloneable validity flag. The two clones — held by the
/// parent `Statement` and the produced executed handle respectively —
/// share the same underlying atomic.
#[derive(Debug, Clone)]
pub(crate) struct ValidityFlag(Arc<AtomicBool>);

impl ValidityFlag {
    /// New flag, initially valid (`true`).
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }

    /// Mark the flag invalid. Idempotent — a second call is a no-op.
    /// Uses `Release` so all writes that happened before this call
    /// (e.g. server-side cleanup) are observable to threads that
    /// later read the flag with `Acquire`.
    pub fn invalidate(&self) {
        self.0.store(false, Ordering::Release);
    }

    /// Returns `Ok(())` if the flag is still valid; otherwise
    /// `Err(InvalidStatementHandle)`. Local-only — no network call.
    /// Uses `Acquire` to synchronise with `invalidate`'s `Release`.
    pub fn check(&self) -> Result<()> {
        if self.0.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::new(
                ErrorCode::InvalidStatementHandle,
                "executed-statement handle has been invalidated by a re-execute on its parent Statement",
            ))
        }
    }
}

impl Default for ValidityFlag {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_flag_is_valid() {
        let f = ValidityFlag::new();
        assert!(f.check().is_ok());
    }

    #[test]
    fn invalidate_flips_check() {
        let f = ValidityFlag::new();
        f.invalidate();
        let err = f.check().unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidStatementHandle);
    }

    #[test]
    fn clone_shares_state() {
        let parent = ValidityFlag::new();
        let child = parent.clone();
        assert!(parent.check().is_ok());
        assert!(child.check().is_ok());
        parent.invalidate();
        assert!(parent.check().is_err());
        assert!(child.check().is_err());
    }

    #[test]
    fn invalidate_is_idempotent() {
        let f = ValidityFlag::new();
        f.invalidate();
        f.invalidate();
        f.invalidate();
        assert!(f.check().is_err());
    }
}
