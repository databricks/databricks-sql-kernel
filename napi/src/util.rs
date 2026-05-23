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

//! Shared helpers — one place for the `catch_unwind` wrapping that
//! every async entry point goes through (pattern #8 in the napi-rs
//! patterns doc). One helper, called once per entry point — DRY.
//!
//! Why a helper rather than a macro: helper + `async move {}` reads
//! better at call sites and keeps the stack trace shallow when a panic
//! actually fires (a macro would expand into the caller's body).

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures::FutureExt;
use napi::{Error as NapiError, Result as NapiResult, Status};

/// Run `fut` and convert any panic the future raises into a
/// `napi::Error` so the JS caller sees a rejected promise instead of
/// the Node process aborting.
///
/// `catch_unwind` does not catch `std::process::abort`, double-panic,
/// or allocator OOM — those still bring down the process. That's by
/// design: a corrupted process state isn't something we can pretend to
/// recover from.
pub(crate) async fn guarded<F, T>(fut: F) -> NapiResult<T>
where
    F: Future<Output = NapiResult<T>>,
{
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(res) => res,
        Err(panic) => Err(NapiError::new(
            Status::GenericFailure,
            format!("panic in native binding: {}", panic_payload_msg(panic)),
        )),
    }
}

/// Best-effort downcast of a panic payload to a human-readable string.
/// `panic!("…")` produces `&'static str` or `String`; the rest fall
/// through to a generic marker so the JS caller still sees *something*.
fn panic_payload_msg(p: Box<dyn Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = p.downcast_ref::<String>() {
        return s.clone();
    }
    "non-string panic payload".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn guarded_returns_inner_ok() {
        let result: NapiResult<i32> = guarded(async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn guarded_passes_through_inner_err() {
        // An `Err` returned by the future flows out unchanged — only
        // panics get rewrapped. Status::Cancelled distinguishes from
        // the panic path's Status::GenericFailure.
        let result: NapiResult<()> =
            guarded(async { Err(NapiError::new(Status::Cancelled, "inner")) }).await;
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::Cancelled);
        assert_eq!(err.reason, "inner");
    }

    #[tokio::test]
    async fn guarded_recovers_static_str_panic() {
        let result: NapiResult<()> = guarded(async {
            panic!("static str panic");
        })
        .await;
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::GenericFailure);
        assert!(
            err.reason.contains("static str panic"),
            "expected panic message in reason, got: {}",
            err.reason
        );
        assert!(err.reason.starts_with("panic in native binding: "));
    }

    #[tokio::test]
    async fn guarded_recovers_owned_string_panic() {
        let result: NapiResult<()> = guarded(async {
            let msg = format!("owned String panic at {}", 42);
            panic!("{}", msg);
        })
        .await;
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::GenericFailure);
        assert!(
            err.reason.contains("owned String panic at 42"),
            "expected formatted panic in reason, got: {}",
            err.reason
        );
    }

    /// Compile-time guard: `panic = "abort"` MUST NOT be active for
    /// any profile this crate builds under. `catch_unwind` (which
    /// `guarded` depends on) becomes a process-aborting no-op under
    /// `panic = "abort"`, turning a recoverable per-statement panic
    /// into a fleet-wide Node-process exit.
    ///
    /// `compile_error!` inside a `#[cfg(panic = "abort")]` block fails
    /// the build with a clear message rather than letting tests pass
    /// against a binary that would crash production. The
    /// `#[allow(unexpected_cfgs)]` is so the `cfg(panic = ...)` check
    /// itself is recognised by rustc's lint as a known cfg key.
    #[allow(unexpected_cfgs)]
    const _: () = {
        #[cfg(panic = "abort")]
        compile_error!(
            "panic = \"abort\" disables catch_unwind, breaking util::guarded \
             and turning recoverable per-statement panics into Node-process \
             aborts. See [profile.release] comment in napi/Cargo.toml."
        );
    };

    #[tokio::test]
    async fn guarded_recovers_non_string_panic() {
        // panic_any with a non-(str|String) payload — these fall
        // through to the "non-string panic payload" branch. Important
        // because third-party crates sometimes panic with custom types.
        let result: NapiResult<()> = guarded(async {
            std::panic::panic_any(42i32);
        })
        .await;
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::GenericFailure);
        assert!(
            err.reason.contains("non-string panic payload"),
            "expected non-string marker in reason, got: {}",
            err.reason
        );
    }
}
