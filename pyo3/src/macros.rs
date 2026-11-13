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

//! Crate-internal macros that collapse the three patterns every
//! wrapper method repeats:
//!
//! - `py.detach(|| runtime.block_on(future)).map_err(|e| to_py_err(py, e))?`
//!   becomes `run_kernel!(py, runtime, future)` (also a `_block`
//!   form for closures that need to capture more than the future).
//! - `self.inner.as_ref().ok_or_else(|| closed_err("X"))?` becomes
//!   `alive!(self, "X")` (immutable) / `alive_mut!(self, "X")`
//!   (mutable).
//!
//! Macros over a helper function so the captured borrow patterns
//! stay flexible at each call site without HRTB gymnastics, and so
//! the macro can expand to a `?`-terminating expression usable
//! anywhere — including inside a `let` binding or as a function
//! tail expression.

/// Drive a kernel future on the session runtime with the GIL
/// released, mapping kernel `Error`s into `KernelError`s.
///
/// Expands to an expression of type `T` (the future's `Ok` value)
/// or short-circuits the enclosing function with `?`.
///
/// ```ignore
/// let session = run_kernel!(py, runtime, KSession::open(config))?;
/// ```
#[macro_export]
macro_rules! run_kernel {
    ($py:expr, $runtime:expr, $future:expr) => {{
        let py = $py;
        py.detach(|| $runtime.block_on($future))
            .map_err(|e| $crate::errors::to_py_err(py, e))
    }};
}

/// Like [`run_kernel!`], but periodically re-acquires the GIL to poll
/// Python signal handlers so a long-running blocking call (e.g. a sync
/// `execute()`) can be interrupted by `KeyboardInterrupt` (Ctrl-C).
///
/// The bare `py.detach(|| block_on(fut))` form runs the future to
/// completion with the GIL released; the main thread is parked in the
/// FFI call, so a SIGINT-set Python signal flag isn't observed until
/// the call returns. This variant instead drives the future in ~100ms
/// slices: each slice runs with the GIL released, and on each timeout
/// the GIL is re-acquired to call [`Python::check_signals`], which
/// raises (e.g. `KeyboardInterrupt`) if a handler is pending. On
/// interrupt the future is dropped — which fires the kernel's
/// mid-execute cancel guard, so the server-side statement is cancelled
/// too, not just abandoned.
///
/// Only Python's main thread runs signal handlers, so on any other thread
/// the future runs in one GIL-released slice: re-acquiring the GIL every
/// tick there costs a GIL hand-off and checks nothing.
///
/// `$runtime` must be a `&tokio::runtime::Runtime` (so the future can
/// be driven across slices). Expands to a `PyResult<T>` (the future's
/// `Ok` value), mapping kernel `Error`s via `to_py_err` and surfacing
/// any signal `PyErr` directly.
#[macro_export]
macro_rules! run_kernel_interruptible {
    ($py:expr, $runtime:expr, $future:expr) => {{
        let py = $py;
        let runtime = $runtime;
        // 100ms balances interrupt latency against per-tick overhead.
        const TICK: ::std::time::Duration = ::std::time::Duration::from_millis(100);
        let mut fut = ::std::boxed::Box::pin($future);
        let interruptible = $crate::gil::on_main_thread(py);
        loop {
            if !interruptible {
                break py
                    .detach(|| runtime.block_on(&mut fut))
                    .map_err(|e| $crate::errors::to_py_err(py, e));
            }
            let step = py.detach(|| {
                runtime.block_on(async {
                    match ::tokio::time::timeout(TICK, &mut fut).await {
                        Ok(done) => Some(done),
                        Err(_elapsed) => None,
                    }
                })
            });
            match step {
                // Future completed within this slice.
                Some(result) => break result.map_err(|e| $crate::errors::to_py_err(py, e)),
                // Slice timed out: poll Python signals. A pending
                // handler (e.g. KeyboardInterrupt) returns Err, which
                // we propagate — dropping `fut` fires the kernel's
                // mid-execute cancel guard.
                None => {
                    if let Err(signal_err) = py.check_signals() {
                        break Err(signal_err);
                    }
                }
            }
        }
    }};
}

/// Variant that accepts a block expression — useful when the future
/// is constructed inline from borrowed state that can't escape the
/// `detach` closure cleanly with the simpler form. Same shape, same
/// error mapping.
///
/// ```ignore
/// let stream = run_kernel_block!(py, runtime, {
///     let stream = inner.exec.result_stream_mut();
///     stream.next_batch()
/// })?;
/// ```
#[macro_export]
macro_rules! run_kernel_block {
    ($py:expr, $runtime:expr, $body:block) => {{
        let py = $py;
        py.detach(|| $runtime.block_on(async $body))
            .map_err(|e| $crate::errors::to_py_err(py, e))
    }};
}

/// Run a synchronous helper (e.g. `fetch::next_batch` / `fetch::drain`,
/// which `block_on` internally) with the GIL released, mapping kernel
/// errors into `KernelError`.
///
/// Same `py.detach` + `to_py_err` shape as `run_kernel!`, but skips
/// the `block_on` wrapper since the callee already owns it.
///
/// ```ignore
/// let batch = run_sync_kernel!(py, fetch::next_batch(&runtime, stream))?;
/// ```
#[macro_export]
macro_rules! run_sync_kernel {
    ($py:expr, $call:expr) => {{
        let py = $py;
        py.detach(|| $call)
            .map_err(|e| $crate::errors::to_py_err(py, e))
    }};
}

/// Borrow `self.inner` immutably or short-circuit with a
/// `KernelError("<handle> is closed", code=InvalidStatementHandle)`.
///
/// ```ignore
/// let inner = alive!(self, "Session");
/// ```
#[macro_export]
macro_rules! alive {
    ($self:expr, $name:literal) => {
        $self
            .inner
            .as_ref()
            .ok_or_else(|| $crate::errors::closed_err($name))?
    };
}

/// Mutable counterpart to `alive!`. Same semantics, `&mut`.
#[macro_export]
macro_rules! alive_mut {
    ($self:expr, $name:literal) => {
        $self
            .inner
            .as_mut()
            .ok_or_else(|| $crate::errors::closed_err($name))?
    };
}
