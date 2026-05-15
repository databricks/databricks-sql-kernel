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
