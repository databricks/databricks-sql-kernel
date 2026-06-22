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

//! Copy-into-cage Arrow handoff across the napi boundary (Flavour A).
//!
//! The default `fetch_next_batch` path (`result.rs::encode_ipc_stream`)
//! re-serialises every `RecordBatch` to Arrow IPC bytes and copies them
//! into V8 — three O(uncompressed-result-size) passes (serialize → napi
//! Buffer copy → JS `RecordBatchReader` decode) that the Python binding
//! avoids entirely via pyarrow's Arrow C Data Interface pointer handoff.
//!
//! This module is the Node analogue. Instead of IPC bytes, it ships the
//! batch's Arrow buffers across the boundary as **in-cage** (V8-allocated)
//! `ArrayBuffer`s: each Arrow buffer is `memcpy`'d into a fresh
//! `napi_create_arraybuffer(len)` allocation inside V8's heap. This is
//! cage-safe (works under the V8 sandbox / Electron, which reject external
//! buffers) and GC-clean — V8 owns the bytes, so no Arc/finalizer is
//! needed. It costs one plain copy per buffer but still avoids the IPC
//! path's serialize + decode passes.
//!
//! The JS side (`lib/kernel/KernelArrowImport.ts`) rebuilds an
//! `apache-arrow` `RecordBatch` from these buffers via `makeData`, driven
//! by the result schema it already decodes once. The buffer→slot mapping
//! and `offset` semantics mirror Arrow's own C Data Interface export
//! (`arrow-data/src/ffi.rs`), so the reconstructed batch is byte-identical
//! to what `RecordBatchReader` would have produced from the IPC stream.
//!
//! The caller (`statement.rs` / `async_statement.rs`) guarantees the batch
//! is offset-0 compact (any sliced array is concat-compacted before it
//! reaches here), so `build_node` never has to apply a logical `offset`
//! other than 0 to the values — the JS importer's `makeData` cannot honour
//! a non-zero offset.

use arrow_array::{Array, RecordBatch};
use arrow_buffer::{Buffer, MutableBuffer, NullBuffer};
use arrow_data::ArrayData;
use arrow_schema::ArrowError;
use arrow_select::concat::concat_batches;
use napi::bindgen_prelude::{ToNapiValue, TypeName};
use napi::{sys, Env, JsObject, NapiRaw, Result as NapiResult, ValueType};

/// Does any array in the batch (recursively, including child arrays of
/// struct / list / map) carry a non-zero logical `offset`? Such an array is
/// a *slice* into a shared backing buffer; the JS importer's `makeData`
/// always builds at offset 0, so a sliced array would silently read from
/// the wrong element. The kernel compacts these to offset 0 before the
/// copycage handoff (see [`compact_to_offset_zero`]).
fn array_data_has_offset(data: &ArrayData) -> bool {
    data.offset() != 0 || data.child_data().iter().any(array_data_has_offset)
}

/// Return an offset-0 compact copy of `batch` if any of its arrays (or their
/// children) is offset-sliced; otherwise return the batch unchanged.
///
/// `concat_batches` over a single-element slice rematerialises every array
/// into a fresh, offset-0 backing buffer — the cheap, correct way to drop a
/// logical offset that `makeData` cannot honour. Copycage already copies
/// every buffer into the cage, so this extra compaction only costs anything
/// in the (rare) sliced case, and even then it is one in-process Arrow copy.
pub(crate) fn compact_to_offset_zero(batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
    let needs_compaction = batch
        .columns()
        .iter()
        .any(|c| array_data_has_offset(&c.to_data()));
    if !needs_compaction {
        return Ok(batch);
    }
    let schema = batch.schema();
    concat_batches(&schema, std::slice::from_ref(&batch))
}

/// A `RecordBatch` whose buffers cross the napi boundary as in-cage copies
/// (Flavour A). Owns the batch (a cheap Arc-based `RecordBatch::clone`) so
/// it can be returned from an `async fn`; the actual JS-value construction
/// happens in `ToNapiValue::to_napi_value`, which runs on the JS thread
/// where a real `Env` is available. Each per-buffer `ArrayBuffer` is
/// V8-allocated and the bytes are `memcpy`'d in, so it is cage-safe and
/// needs no finalizer.
pub struct CopyCageBatch {
    pub(crate) batch: RecordBatch,
}

impl CopyCageBatch {
    pub(crate) fn new(batch: RecordBatch) -> Self {
        Self { batch }
    }
}

/// Align a `NullBuffer` to `data_offset` so a single element `offset` on
/// the JS-side `Data` applies uniformly to values and validity — the exact
/// contract Arrow's own C Data Interface export uses. Lifted from
/// `arrow-data/src/ffi.rs::align_nulls`:
///   * offsets already match  → return the buffer as-is (zero-copy)
///   * `data_offset == 0`     → bit-slice to a fresh offset-0 buffer
///   * otherwise              → repack into an aligned buffer
///
/// In practice the caller compacts every batch to offset 0 before it
/// reaches here, so the first/second arm fire; the third arm is retained
/// for defence-in-depth (it matches upstream Arrow exactly).
fn align_nulls(data_offset: usize, nulls: Option<&NullBuffer>) -> Option<Buffer> {
    let nulls = nulls?;
    if data_offset == nulls.offset() {
        // Validity already starts at the same logical zero as values.
        Some(nulls.buffer().clone())
    } else if data_offset == 0 {
        Some(nulls.inner().sliced())
    } else {
        let buf = MutableBuffer::collect_bool(data_offset + nulls.len(), |idx| {
            idx >= data_offset && nulls.is_valid(idx - data_offset)
        });
        Some(buf.into())
    }
}

/// Create an in-cage (V8-allocated) `ArrayBuffer` and copy an Arrow
/// `Buffer`'s bytes into it (Flavour A). `create_arraybuffer(len)` routes to
/// `napi_create_arraybuffer`, which allocates inside V8's heap — cage-safe,
/// and V8 owns the bytes so no finalizer / Arc-bump is needed (the source
/// `buf` is dropped right after the copy). One plain `memcpy` per buffer.
///
/// `create_arraybuffer(0)` yields a fresh, distinct empty `ArrayBuffer`
/// (a null data pointer for which `AsMut` returns an empty slice), so the
/// copy below is a harmless no-op for zero-length buffers — no special
/// case needed, and no buffer is ever aliased or shared.
fn arraybuffer_copy_to_cage(env: &Env, buf: &Buffer) -> NapiResult<napi::JsArrayBuffer> {
    let src = buf.as_slice();
    let mut value = env.create_arraybuffer(src.len())?;
    let dst: &mut [u8] = value.as_mut();
    dst.copy_from_slice(src);
    Ok(value.into_raw())
}

/// Recursively build the JS descriptor object for one `ArrayData` node:
///
/// ```text
/// { length, offset, nullCount,
///   validity?: ArrayBuffer,      // omitted when the node has no null buffer
///   buffers:  ArrayBuffer[],     // the type's data buffers, in Arrow order
///   children: Node[] }           // child ArrayData (struct fields / list values)
/// ```
///
/// The buffer→slot mapping is left to the JS importer, which keys off the
/// result schema's per-field `DataType`; here we just expose the raw tree
/// in canonical Arrow order (`ArrayData::buffers()` / `child_data()`).
fn build_node(env: &Env, data: &ArrayData) -> NapiResult<JsObject> {
    let mut node = env.create_object()?;
    node.set_named_property("length", env.create_int64(data.len() as i64)?)?;
    node.set_named_property("offset", env.create_int64(data.offset() as i64)?)?;
    node.set_named_property("nullCount", env.create_int64(data.null_count() as i64)?)?;

    if let Some(validity) = align_nulls(data.offset(), data.nulls()) {
        node.set_named_property("validity", arraybuffer_copy_to_cage(env, &validity)?)?;
    }

    let data_buffers = data.buffers();
    let mut buffers = env.create_array_with_length(data_buffers.len())?;
    for (i, buf) in data_buffers.iter().enumerate() {
        buffers.set_element(i as u32, arraybuffer_copy_to_cage(env, buf)?)?;
    }
    node.set_named_property("buffers", buffers)?;

    let child_data = data.child_data();
    let mut children = env.create_array_with_length(child_data.len())?;
    for (i, child) in child_data.iter().enumerate() {
        children.set_element(i as u32, build_node(env, child)?)?;
    }
    node.set_named_property("children", children)?;

    Ok(node)
}

/// Build `{ numRows, columns: Node[] }` for `batch`, minting each per-buffer
/// `ArrayBuffer` as an in-cage copy.
unsafe fn batch_to_napi_value(
    raw_env: sys::napi_env,
    batch: RecordBatch,
) -> NapiResult<sys::napi_value> {
    let env = unsafe { Env::from_raw(raw_env) };

    let mut out = env.create_object()?;
    out.set_named_property("numRows", env.create_int64(batch.num_rows() as i64)?)?;

    let mut columns = env.create_array_with_length(batch.num_columns())?;
    for (i, column) in batch.columns().iter().enumerate() {
        let data = column.to_data();
        columns.set_element(i as u32, build_node(&env, &data)?)?;
    }
    out.set_named_property("columns", columns)?;

    Ok(unsafe { out.raw() })
}

/// Surfaces in the generated `index.d.ts` as the (opaque-to-TS) return type
/// of `fetchNextBatchCopycage`. The JS importer (`KernelArrowImport.ts`)
/// re-types the value structurally, so a plain `object` here is sufficient.
impl TypeName for CopyCageBatch {
    fn type_name() -> &'static str {
        "CopyCageBatch"
    }

    fn value_type() -> ValueType {
        ValueType::Object
    }
}

impl ToNapiValue for CopyCageBatch {
    /// Build the `{ numRows, columns: Node[] }` descriptor with in-cage
    /// (V8-allocated, `memcpy`'d) `ArrayBuffer`s — cage-safe and
    /// finalizer-free. Runs on the JS thread, so the `Env` reconstructed
    /// here is the live napi environment needed to mint `ArrayBuffer`s.
    unsafe fn to_napi_value(raw_env: sys::napi_env, val: Self) -> NapiResult<sys::napi_value> {
        unsafe { batch_to_napi_value(raw_env, val.batch) }
    }
}
