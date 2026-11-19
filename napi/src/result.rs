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

//! Arrow IPC payload types crossed across the napi boundary.
//!
//! Per sea-design.md Layer 2: "The binding ships the batch across the
//! FFI as Arrow IPC bytes. The adapter converts those bytes into
//! JavaScript rows…" — so the napi boundary is intentionally narrow:
//! one envelope per batch, one envelope per schema.

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use databricks_sql_kernel::Error as KernelError;
use napi::bindgen_prelude::Buffer;

use crate::error::napi_err_from_kernel;

/// A single Arrow IPC stream payload encoding one record batch (plus
/// the schema header so the JS-side reader is stateless).
#[napi(object)]
pub struct ArrowBatch {
    /// Arrow IPC stream payload (schema header + 1 record-batch
    /// message). Decode with `apache-arrow`'s `RecordBatchReader`.
    pub ipc_bytes: Buffer,
}

/// An Arrow IPC stream payload encoding just the result schema (no
/// record-batch messages). Returned by `Statement.schema()`.
#[napi(object)]
pub struct ArrowSchema {
    /// Arrow IPC stream payload (schema header only, no record-batch
    /// messages). Decode with `apache-arrow`'s `RecordBatchReader` —
    /// the reader will expose the schema and immediately end.
    pub ipc_bytes: Buffer,
}

/// Encode an Arrow schema (and an optional single record batch) as a
/// complete, self-contained Arrow IPC stream payload.
///
/// Shared single source of truth for every batch/schema that crosses
/// the napi boundary: the sync `Statement` path (`schema()` /
/// `fetchNextBatch()` in `statement.rs`) and the async
/// `AsyncResultHandle` path (`async_statement.rs`) both call this, so
/// the two emit byte-identical envelopes and route encode failures
/// through the same kernel error envelope (`napi_err_from_kernel`) —
/// the TS reviver sees a typed `KernelError(code='Internal')` rather
/// than a bare `GenericFailure` string. Lives here in `result.rs`
/// because this module already owns `ArrowBatch` / `ArrowSchema`, the
/// payload types these bytes are wrapped in.
///
/// Returning a self-contained IPC stream per call is wasteful
/// header-wise but lets the JS adapter stay stateless — it decodes
/// each `ipcBytes` independently via the same `apache-arrow`
/// `RecordBatchReader` path.
pub(crate) fn encode_ipc_stream(
    schema: &SchemaRef,
    batch: Option<&RecordBatch>,
) -> napi::Result<Vec<u8>> {
    fn encode_err(stage: &str, e: impl std::fmt::Display) -> napi::Error {
        napi_err_from_kernel(
            KernelError::internal().message(format!("Arrow IPC writer {stage} failed: {e}")),
        )
    }
    // Pre-size the buffer so a typical 500KB batch doesn't reallocate
    // ~17 times growing from `Vec::new()`'s default capacity of 0.
    // `get_array_memory_size` is the kernel-side memory footprint (close
    // enough to the IPC-encoded size for capacity-hint purposes — IPC
    // adds ~hundreds of bytes of framing per batch, not orders of
    // magnitude). 4 KiB slack covers the schema-header on the
    // schema-only path (`batch = None`) and the framing overhead on
    // the schema+batch path.
    let estimated = batch.map(|rb| rb.get_array_memory_size()).unwrap_or(0) + 4096;
    let mut buf: Vec<u8> = Vec::with_capacity(estimated);
    {
        let mut writer =
            StreamWriter::try_new(&mut buf, schema).map_err(|e| encode_err("init", e))?;
        if let Some(rb) = batch {
            writer.write(rb).map_err(|e| encode_err("write", e))?;
        }
        writer.finish().map_err(|e| encode_err("finish", e))?;
    }
    Ok(buf)
}

#[napi(string_enum = "snake_case")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeCompletion {
    NotVolume,
    NeedsInputStream,
    Completed,
}

impl From<databricks_sql_kernel::VolumeCompletion> for VolumeCompletion {
    fn from(value: databricks_sql_kernel::VolumeCompletion) -> Self {
        match value {
            databricks_sql_kernel::VolumeCompletion::NotVolume => Self::NotVolume,
            databricks_sql_kernel::VolumeCompletion::NeedsInputStream => Self::NeedsInputStream,
            databricks_sql_kernel::VolumeCompletion::Completed => Self::Completed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_ipc::reader::StreamReader;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    #[test]
    fn encode_ipc_stream_schema_only_yields_zero_batches() {
        let schema = make_schema();
        let bytes = encode_ipc_stream(&schema, None).expect("encode succeeds");
        let reader = StreamReader::try_new(bytes.as_slice(), None).expect("reader opens");
        let decoded_schema = reader.schema();
        let batches: Vec<_> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("decode succeeds");
        assert_eq!(decoded_schema.fields(), schema.fields());
        assert_eq!(batches.len(), 0, "schema-only payload yields 0 batches");
    }

    #[test]
    fn encode_ipc_stream_schema_plus_batch_round_trips() {
        let schema = make_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("batch builds");
        let bytes = encode_ipc_stream(&schema, Some(&batch)).expect("encode succeeds");
        let reader = StreamReader::try_new(bytes.as_slice(), None).expect("reader opens");
        let decoded: Vec<_> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("decode succeeds");
        assert_eq!(decoded.len(), 1, "exactly one batch decoded");
        assert_eq!(decoded[0].num_rows(), 3);
        assert_eq!(decoded[0].schema().fields(), schema.fields());
    }
}
