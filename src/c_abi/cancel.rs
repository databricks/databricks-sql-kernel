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

//! Generic cooperative cancel token for the blocking C-ABI entry points.
//!
//! The v0 C ABI drives every async operation through
//! [`block_on_ffi`](crate::c_abi::block_on_ffi), which blocks the calling
//! (native) thread until the future completes. That thread cannot observe a
//! host-side deadline/cancel (a Go `context.Context`, an ODBC `SQLCancel`)
//! mid-call, so a hung connect or a hung result-stream fetch blocks the caller
//! past its deadline — the runtime-contract gap the host drivers track as
//! mid-fetch (hung CloudFetch chunk) and session-connect deadlines.
//!
//! This module adds one primitive that closes both: a [`kernel_cancel_token_t`]
//! the host creates before a blocking call, passes into a `*_cancellable`
//! variant of that call, and fires from *another* thread when its deadline
//! elapses. The cancellable entry point runs its future under
//! [`block_on_ffi_cancellable`](crate::c_abi::block_on_ffi_cancellable), which
//! races the future against the token and — on a fire — **drops the future**.
//! Because the kernel's HTTP client is `reqwest` and its futures cancel by
//! being dropped (the crate-wide model; see the CloudFetch streaming provider's
//! own [`CancellationToken`] use), dropping the future genuinely aborts the
//! in-flight request rather than merely stopping the wait.
//!
//! The token wraps [`tokio_util::sync::CancellationToken`], which is cheap to
//! clone (an `Arc` internally) and safe to fire from any thread — the same
//! shape as the statement canceller ([`kernel_statement_canceller_t`]), and for
//! the same reason (the blocking call runs on one thread, the cancel on
//! another). Unlike the statement canceller, firing is purely local (flip an
//! atomic + wake waiters); it dispatches no RPC, so it never blocks.
//!
//! One token drives at most one in-flight call at a time. Reuse across
//! sequential calls is allowed, but a token that has already been fired stays
//! fired (a `CancellationToken` is one-shot), so create a fresh token per
//! logical operation — exactly as a Go caller creates a fresh watcher per
//! `ctx`.
//!
//! [`kernel_statement_canceller_t`]: crate::c_abi::statement::kernel_statement_canceller_t
//! [`CancellationToken`]: tokio_util::sync::CancellationToken

#![allow(non_camel_case_types)]

use super::error::store_invalid_arg;
use super::{catch_ffi, KernelStatusCode};
use tokio_util::sync::CancellationToken;

/// Opaque cancel-token handle. Boxes a [`CancellationToken`] the host fires
/// from a watcher thread to interrupt a blocking `*_cancellable` C-ABI call.
#[repr(C)]
pub struct kernel_cancel_token_t {
    _private: [u8; 0],
}

/// Owned box behind a [`kernel_cancel_token_t`].
struct CancelTokenHandle {
    token: CancellationToken,
}

/// Crate-internal: borrow the [`CancellationToken`] out of a raw handle, or
/// `None` when the handle pointer is null. The returned reference is only used
/// to `clone()` the token (a cheap `Arc` bump) into the future's `select!`, so
/// the caller keeps ownership of the box and may still fire it concurrently.
///
/// # Safety
/// `token` must be null or a live handle from [`kernel_cancel_token_new`].
pub(crate) unsafe fn token_ref<'a>(
    token: *const kernel_cancel_token_t,
) -> Option<&'a CancellationToken> {
    (token as *const CancelTokenHandle)
        .as_ref()
        .map(|h| &h.token)
}

/// Create a fresh, un-fired cancel token. On success `*out` holds an owned
/// handle the caller releases with [`kernel_cancel_token_free`], and may fire
/// with [`kernel_cancel_token_cancel`] from any thread.
///
/// # Safety
/// `out` must be a valid, writable pointer to a `kernel_cancel_token_t*`.
#[no_mangle]
pub unsafe extern "C" fn kernel_cancel_token_new(
    out: *mut *mut kernel_cancel_token_t,
) -> KernelStatusCode {
    catch_ffi(|| {
        if out.is_null() {
            return store_invalid_arg("kernel_cancel_token_new: out must not be null");
        }
        let handle = CancelTokenHandle {
            token: CancellationToken::new(),
        };
        std::ptr::write(
            out,
            Box::into_raw(Box::new(handle)) as *mut kernel_cancel_token_t,
        );
        KernelStatusCode::Success
    })
}

/// Fire the token, requesting cancellation of whatever blocking
/// `*_cancellable` call it was passed to. Idempotent (a `CancellationToken` is
/// one-shot; a second fire is a no-op) and safe to call concurrently with the
/// blocking call the token drives — that is the whole point: the blocking call
/// runs on one thread and this fires from another.
///
/// Purely local: flips the token's atomic and wakes the `select!` waiting on
/// it. Dispatches no RPC, so it never blocks and never fails on a live handle.
///
/// # Safety
/// `token` must be a live handle from [`kernel_cancel_token_new`], not yet
/// freed. It is safe to call while a `*_cancellable` call driven by this token
/// is in flight; do not [`kernel_cancel_token_free`] the handle until both this
/// call and that blocking call have returned.
#[no_mangle]
pub unsafe extern "C" fn kernel_cancel_token_cancel(
    token: *mut kernel_cancel_token_t,
) -> KernelStatusCode {
    catch_ffi(|| {
        let Some(handle) = (token as *const CancelTokenHandle).as_ref() else {
            return store_invalid_arg("kernel_cancel_token_cancel: token must not be null");
        };
        handle.token.cancel();
        KernelStatusCode::Success
    })
}

/// Report whether the token has been fired, writing `true`/`false` to `*out`.
/// A local read of the token's atomic — no RPC, never blocks. Symmetric with
/// [`kernel_session_is_open`](crate::c_abi::session::kernel_session_is_open);
/// lets a host (or its tests) observe the token state it set. Does not borrow
/// ownership; the caller still owns `token`.
///
/// # Safety
/// `token` must be a live handle from [`kernel_cancel_token_new`], not yet
/// freed; `out` must be a writable `bool*`.
#[no_mangle]
pub unsafe extern "C" fn kernel_cancel_token_is_cancelled(
    token: *const kernel_cancel_token_t,
    out: *mut bool,
) -> KernelStatusCode {
    catch_ffi(|| {
        let Some(handle) = (token as *const CancelTokenHandle).as_ref() else {
            return store_invalid_arg("kernel_cancel_token_is_cancelled: token must not be null");
        };
        if out.is_null() {
            return store_invalid_arg("kernel_cancel_token_is_cancelled: out must not be null");
        }
        std::ptr::write(out, handle.token.is_cancelled());
        KernelStatusCode::Success
    })
}

/// Free a cancel-token handle. Does not touch any call the token drove.
///
/// # Safety
/// `token` must have been returned by [`kernel_cancel_token_new`] and not
/// previously freed. The token is single-owner for teardown: no other thread
/// may be inside [`kernel_cancel_token_cancel`] on this handle, and no
/// `*_cancellable` call driven by it may be in flight, when it is freed.
#[no_mangle]
pub unsafe extern "C" fn kernel_cancel_token_free(
    token: *mut kernel_cancel_token_t,
) -> KernelStatusCode {
    catch_ffi(|| {
        if token.is_null() {
            return store_invalid_arg("kernel_cancel_token_free: token must not be null");
        }
        drop(Box::from_raw(token as *mut CancelTokenHandle));
        KernelStatusCode::Success
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// new → cancel → free round-trips, and the borrowed token reflects the
    /// fire (the property the cancellable entry points rely on).
    #[test]
    fn new_cancel_free_round_trip() {
        let mut tok: *mut kernel_cancel_token_t = std::ptr::null_mut();
        let rc = unsafe { kernel_cancel_token_new(&mut tok) };
        assert_eq!(rc, KernelStatusCode::Success);
        assert!(!tok.is_null());

        // Not fired yet — observe via both the borrow and the C accessor.
        let borrowed = unsafe { token_ref(tok) }.expect("live token");
        assert!(!borrowed.is_cancelled());
        let mut fired = true;
        assert_eq!(
            unsafe { kernel_cancel_token_is_cancelled(tok, &mut fired) },
            KernelStatusCode::Success
        );
        assert!(!fired, "is_cancelled reports false before a fire");

        // Fire it, then observe the flip through the borrow AND the accessor.
        let rc = unsafe { kernel_cancel_token_cancel(tok) };
        assert_eq!(rc, KernelStatusCode::Success);
        assert!(borrowed.is_cancelled(), "cancel() must flip the token");
        assert_eq!(
            unsafe { kernel_cancel_token_is_cancelled(tok, &mut fired) },
            KernelStatusCode::Success
        );
        assert!(fired, "is_cancelled reports true after a fire");

        // Second fire is a no-op (one-shot), still Success.
        let rc = unsafe { kernel_cancel_token_cancel(tok) };
        assert_eq!(rc, KernelStatusCode::Success);

        let rc = unsafe { kernel_cancel_token_free(tok) };
        assert_eq!(rc, KernelStatusCode::Success);
    }

    /// Null-pointer guards on every entry point return `InvalidArgument`,
    /// never crash.
    #[test]
    fn null_guards_return_invalid_argument() {
        let rc = unsafe { kernel_cancel_token_new(std::ptr::null_mut()) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);
        let rc = unsafe { kernel_cancel_token_cancel(std::ptr::null_mut()) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);
        let mut fired = false;
        let rc = unsafe { kernel_cancel_token_is_cancelled(std::ptr::null(), &mut fired) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);
        let rc = unsafe { kernel_cancel_token_free(std::ptr::null_mut()) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);
    }

    /// `token_ref` on a null handle yields `None` (used by the cancellable
    /// entry points to treat a null token as "no cancellation").
    #[test]
    fn token_ref_null_is_none() {
        assert!(unsafe { token_ref(std::ptr::null()) }.is_none());
    }
}
