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

//! Arrow IPC parsing utilities for result data.
//!
//! This module handles parsing Arrow IPC Streaming format bytes from various sources:
//! - CloudFetch downloads from cloud storage
//! - Inline Arrow data embedded in API responses
//!
//! Supports optional LZ4 decompression based on the result manifest.

use crate::kernel_error::{Error, Result};
use crate::types::sea::CompressionCodec;
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use lz4_flex::frame::FrameDecoder;
use std::io::{self, BufReader, Cursor, Read};

/// Streaming reader over a buffer that may concatenate multiple LZ4 frames.
///
/// Databricks LZ4_FRAME payloads pack one frame per server-side Arrow record
/// batch, end-to-end in a single body. `FrameDecoder::read` returns EOF at
/// each frame's end-marker, so a single `FrameDecoder` cannot read across
/// frame boundaries. This adapter peels frames lazily: as soon as the
/// current frame is exhausted, it advances past the consumed bytes and
/// starts a new `FrameDecoder` on whatever follows, transparently to the
/// caller.
///
/// Used by `parse_arrow_ipc` to pipe LZ4-compressed bodies directly into
/// `arrow_ipc::reader::StreamReader` without ever materializing the full
/// decompressed payload as a single `Vec<u8>`. Mirrors the JDBC driver's
/// switch from `decompress -> byte[]` to a streaming `LZ4FrameInputStream`.
///
/// What this does and does not bound: the win is removing the >2 GiB
/// single contiguous-buffer ceiling (an `i32`-indexed `byte[]` in JDBC,
/// a single materialized `Vec<u8>` here) and a redundant memcpy. Peak
/// memory for a chunk drops from `(decompressed bytes + decoded batches)`
/// to `(decoded batches for this chunk)`. It does NOT bound the total
/// decoded result size — a query that decompresses to ~10 GB will still
/// hold ~10 GB of `RecordBatch` data in memory once
/// `parse_arrow_ipc` finishes, because the function returns a
/// `Vec<RecordBatch>` covering the whole chunk. Per-batch streaming to
/// the consumer would require a different signature (an iterator over
/// `Result<RecordBatch>`); out of scope for this fix.
struct MultiFrameLz4Reader<'a> {
    /// Bytes that have not yet been consumed by any frame decoder.
    remaining: &'a [u8],
    /// Active decoder for the current frame, if any. `None` between
    /// frames (or before the first one).
    current: Option<FrameDecoder<Cursor<&'a [u8]>>>,
}

impl<'a> MultiFrameLz4Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            remaining: data,
            current: None,
        }
    }
}

impl<'a> Read for MultiFrameLz4Reader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Per the `std::io::Read` contract, `Ok(0)` is allowed when
        // `buf.len() == 0`. Without this guard, the EOF-on-frame branch
        // below would interpret the inner decoder's `Ok(0)` as "frame
        // exhausted" and advance `remaining` past a still-live frame,
        // corrupting subsequent reads. Arrow's `StreamReader` happens
        // never to issue a zero-length read today (verified upstream),
        // but the bug would be silent if it ever did.
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            if let Some(decoder) = self.current.as_mut() {
                let n = decoder.read(buf)?;
                if n > 0 {
                    return Ok(n);
                }
                // EOF on this frame — recover the consumed-byte count
                // from the inner cursor and advance `remaining`.
                let consumed = decoder.get_ref().position() as usize;
                if consumed == 0 {
                    // Defensive: a zero-progress decoder on a non-empty
                    // input would otherwise spin forever. Prefix the
                    // message with `[multi-frame-lz4]` so it stays
                    // greppable after `StreamReader` wraps it inside a
                    // generic "Failed to read Arrow batches" envelope
                    // in `parse_arrow_ipc`.
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "[multi-frame-lz4] LZ4 decoder stalled with {} bytes remaining",
                            self.remaining.len()
                        ),
                    ));
                }
                self.remaining = &self.remaining[consumed..];
                self.current = None;
            }
            if self.remaining.is_empty() {
                return Ok(0);
            }
            self.current = Some(FrameDecoder::new(Cursor::new(self.remaining)));
        }
    }
}

/// Parse Arrow IPC data into RecordBatches.
///
/// Handles Arrow IPC streaming format bytes from either:
/// - CloudFetch downloads from cloud storage
/// - Inline Arrow data embedded in API responses (base64-decoded attachment field)
///
/// The data may be optionally LZ4 compressed, as indicated by the compression codec
/// from the ResultManifest (manifest.result_compression field).
/// A single chunk may contain multiple RecordBatches.
///
/// # Arguments
/// * `data` - Raw Arrow IPC bytes (from CloudFetch download or inline attachment)
/// * `compression` - Compression codec used (from manifest.result_compression)
///
/// # Returns
/// Vec of RecordBatches parsed from the Arrow IPC stream
///
/// # Errors
/// - If LZ4 decompression fails
/// - If Arrow IPC parsing fails
pub fn parse_arrow_ipc(data: &[u8], compression: CompressionCodec) -> Result<Vec<RecordBatch>> {
    // Pipe the (possibly LZ4-decompressed) byte stream directly into the
    // Arrow IPC reader. The decompressed body is never materialized as a
    // single `Vec<u8>` — that removes the >2 GiB contiguous-buffer
    // ceiling that JDBC's `byte[] decompressedBytes` path hit on the
    // 10 GB-class query below. The function still returns a
    // `Vec<RecordBatch>` covering the whole chunk, so peak memory once
    // this returns is the chunk's decoded size — not the wire size, but
    // not bounded to a single batch either. See `MultiFrameLz4Reader`
    // doc for the precise envelope.
    //
    // The query that makes this matter:
    // `SELECT repeat('A', 1024*1024) FROM range(10*1024)` — ships
    // ~14 MB compressed, decompresses to ~10 GB. Before this change
    // that decompressed `Vec<u8>` either OOM'd at allocation or tripped
    // the 2 GiB IPC array-offset limit. Mirrors the JDBC fix in
    // `SeaInlineArrowChunkProvider.processResultData` (switch from
    // `byte[] decompressedBytes` to `LZ4FrameInputStream`).
    let reader: Box<dyn Read + '_> = match compression {
        CompressionCodec::Lz4Frame => Box::new(MultiFrameLz4Reader::new(data)),
        CompressionCodec::None => Box::new(Cursor::new(data)),
    };

    // Wrap in `BufReader` so the many small reads `StreamReader` issues
    // (4–8 byte continuation/length prefixes, then FlatBuffer metadata)
    // are coalesced into block-sized reads against `MultiFrameLz4Reader`
    // / `Cursor`. Without this, each prefix crosses the `Box<dyn Read>`
    // vtable and then a `FrameDecoder::read` call — measurable overhead
    // on small results. The previous code read from an in-memory
    // `Cursor<Vec<u8>>` where each `StreamReader` read was a cheap
    // memcpy; `BufReader` restores equivalent behaviour while keeping
    // the streaming decompress.
    let stream = StreamReader::try_new(BufReader::new(reader), None)
        .map_err(|e| Error::io().message(format!("Failed to create Arrow IPC reader: {}", e)))?;

    let batches: Vec<RecordBatch> = stream
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::io().message(format!("Failed to read Arrow batches: {}", e)))?;

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Helper to create test Arrow IPC data
    fn create_test_arrow_ipc(batches: &[RecordBatch]) -> Vec<u8> {
        let schema = batches[0].schema();
        let mut buffer = Vec::new();

        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
            for batch in batches {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
        }

        buffer
    }

    /// Helper to create a test RecordBatch
    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids: Vec<i32> = (0..num_rows as i32).collect();
        let names: Vec<String> = (0..num_rows).map(|i| format!("name_{}", i)).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_parse_uncompressed_arrow_ipc() {
        let batch = create_test_batch(100);
        let ipc_data = create_test_arrow_ipc(std::slice::from_ref(&batch));

        let result = parse_arrow_ipc(&ipc_data, CompressionCodec::None).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 100);
        assert_eq!(result[0].num_columns(), 2);
    }

    #[test]
    fn test_parse_multiple_batches() {
        let batch1 = create_test_batch(50);
        let batch2 = create_test_batch(30);
        let ipc_data = create_test_arrow_ipc(&[batch1, batch2]);

        let result = parse_arrow_ipc(&ipc_data, CompressionCodec::None).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].num_rows(), 50);
        assert_eq!(result[1].num_rows(), 30);
    }

    #[test]
    fn test_parse_compressed_arrow_ipc() {
        use lz4_flex::frame::FrameEncoder;
        use std::io::Write;

        let batch = create_test_batch(100);
        let ipc_data = create_test_arrow_ipc(std::slice::from_ref(&batch));

        // Compress with LZ4 frame format
        let mut compressed = Vec::new();
        {
            let mut encoder = FrameEncoder::new(&mut compressed);
            encoder.write_all(&ipc_data).unwrap();
            encoder.finish().unwrap();
        }

        let result = parse_arrow_ipc(&compressed, CompressionCodec::Lz4Frame).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 100);
    }

    #[test]
    fn test_parse_multi_frame_compressed_arrow_ipc() {
        // Databricks LZ4_FRAME payloads split a single IPC stream across
        // multiple consecutive LZ4 frames in one buffer. Reproduce by
        // splitting one IPC stream's bytes into two halves and encoding
        // each half as its own LZ4 frame. The decoder must peel both
        // frames; FrameDecoder::read_to_end on its own stops at the
        // first frame's end-marker.
        use lz4_flex::frame::FrameEncoder;
        use std::io::Write;

        let batch = create_test_batch(100);
        let ipc_data = create_test_arrow_ipc(std::slice::from_ref(&batch));
        assert!(
            ipc_data.len() > 32,
            "test fixture too small to split meaningfully"
        );
        let mid = ipc_data.len() / 2;

        let encode = |bytes: &[u8]| {
            let mut out = Vec::new();
            let mut enc = FrameEncoder::new(&mut out);
            enc.write_all(bytes).unwrap();
            enc.finish().unwrap();
            out
        };

        let mut multi_frame = encode(&ipc_data[..mid]);
        multi_frame.extend(encode(&ipc_data[mid..]));

        // Sanity-check: feeding only the first frame to the decoder
        // cannot reconstruct the full batch (the IPC stream is
        // truncated). We treat any non-success outcome as the
        // sanity-check passing — the exact error/empty shape from
        // arrow_ipc on a truncated stream isn't part of our contract.
        let only_first = parse_arrow_ipc(&encode(&ipc_data[..mid]), CompressionCodec::Lz4Frame);
        let truncated_yields_batch = matches!(&only_first, Ok(batches) if !batches.is_empty());
        assert!(
            !truncated_yields_batch,
            "first frame alone must not yield a complete batch"
        );

        // Full multi-frame payload must decompress both frames and
        // recover the complete batch.
        let result = parse_arrow_ipc(&multi_frame, CompressionCodec::Lz4Frame).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 100);
    }

    #[test]
    fn test_parse_invalid_data() {
        let invalid_data = b"this is not valid arrow ipc data";

        let result = parse_arrow_ipc(invalid_data, CompressionCodec::None);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_compressed_data() {
        let invalid_data = b"this is not valid lz4 compressed data";

        let result = parse_arrow_ipc(invalid_data, CompressionCodec::Lz4Frame);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_empty_stream() {
        // Create valid but empty Arrow IPC stream
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let mut buffer = Vec::new();

        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
            writer.finish().unwrap();
        }

        let result = parse_arrow_ipc(&buffer, CompressionCodec::None).unwrap();
        assert!(result.is_empty());
    }

    // ──────────────────────────────────────────────────────────────
    // MultiFrameLz4Reader — direct Read-impl tests.
    //
    // The integration coverage above flows through `parse_arrow_ipc`
    // with large buffers, so it hits the happy multi-frame path but
    // can't pin the small-buf / stall / empty-buf branches. The tests
    // below construct the reader directly and call `read()` so each
    // new branch in the Read impl is exercised in isolation.
    // ──────────────────────────────────────────────────────────────

    /// Helper: LZ4-encode `bytes` as a single frame.
    fn encode_lz4_frame(bytes: &[u8]) -> Vec<u8> {
        use lz4_flex::frame::FrameEncoder;
        use std::io::Write;
        let mut out = Vec::new();
        let mut enc = FrameEncoder::new(&mut out);
        enc.write_all(bytes).unwrap();
        enc.finish().unwrap();
        out
    }

    /// Helper: drain a `Read` into a `Vec<u8>` using a fixed-size
    /// buffer. Returns (bytes, read_call_count) so tests can assert on
    /// both the data and the call shape.
    fn drain_with_buf_size(mut r: impl Read, buf_size: usize) -> (Vec<u8>, usize) {
        let mut out = Vec::new();
        let mut buf = vec![0u8; buf_size];
        let mut reads = 0;
        loop {
            match r.read(&mut buf) {
                Ok(0) => return (out, reads),
                Ok(n) => {
                    out.extend_from_slice(&buf[..n]);
                    reads += 1;
                }
                Err(e) => panic!("read failed: {e}"),
            }
        }
    }

    /// `Ok(0)` from `read` when `buf` is empty must NOT be confused
    /// with "frame exhausted" — that path is the documented
    /// std::io::Read contract corner. Without the guard at the top of
    /// the read impl, the EOF-on-frame branch would advance
    /// `remaining` past a still-live frame and corrupt subsequent
    /// reads. Verify by issuing an empty read and then a normal read,
    /// asserting the normal read still returns the full content.
    #[test]
    fn multi_frame_reader_handles_empty_buf_without_corrupting_stream() {
        let payload = b"hello, lz4 frame!";
        let frame = encode_lz4_frame(payload);
        let mut r = MultiFrameLz4Reader::new(&frame);

        // First call with zero-length buf: must return Ok(0) WITHOUT
        // touching the underlying frame state.
        let mut empty: [u8; 0] = [];
        assert_eq!(r.read(&mut empty).unwrap(), 0);

        // Then drain into a real buf; if the first call corrupted
        // state, this would either short-read or fail.
        let (got, _reads) = drain_with_buf_size(&mut r, 64);
        assert_eq!(&got, payload, "stream must be intact after empty read");
    }

    /// Small-buf drain across multiple frames: the reader must
    /// transparently span frame boundaries while honouring a buf
    /// smaller than one frame's output. Mirrors what `BufReader`
    /// internals look like when refilling a small block.
    #[test]
    fn multi_frame_reader_drains_correctly_with_small_buf() {
        // Three frames so we exercise more than one frame transition.
        let p1 = b"first  ";
        let p2 = b"second ";
        let p3 = b"third!!";
        let mut bytes = encode_lz4_frame(p1);
        bytes.extend(encode_lz4_frame(p2));
        bytes.extend(encode_lz4_frame(p3));

        // 4-byte buf is intentionally smaller than each payload
        // (7 bytes) — forces multiple read() calls inside a single
        // frame plus the cross-frame transitions.
        let (got, reads) = drain_with_buf_size(MultiFrameLz4Reader::new(&bytes), 4);
        let mut expected = Vec::new();
        expected.extend_from_slice(p1);
        expected.extend_from_slice(p2);
        expected.extend_from_slice(p3);
        assert_eq!(got, expected, "all three frames must be drained in order");
        assert!(
            reads >= expected.len() / 4,
            "small buf must force multiple read() calls (got {reads} for {} bytes)",
            expected.len()
        );
    }

    /// 3+ concatenated frames must drain correctly into a single
    /// buffer (large-buf case for the multi-frame path). Companion to
    /// `multi_frame_reader_drains_correctly_with_small_buf` which
    /// exercises the small-buf path on the same fixture shape.
    #[test]
    fn multi_frame_reader_handles_three_or_more_frames() {
        let mut bytes = Vec::new();
        let mut expected = Vec::new();
        for i in 0..5 {
            let payload = format!("frame-{i}-content").into_bytes();
            bytes.extend(encode_lz4_frame(&payload));
            expected.extend(payload);
        }

        let (got, _) = drain_with_buf_size(MultiFrameLz4Reader::new(&bytes), 4096);
        assert_eq!(got, expected, "5 frames must drain to concatenated payload");
    }

    /// Stall-path: if a non-empty `remaining` produces a decoder that
    /// reports EOF immediately (consumed == 0), the reader must
    /// surface an `InvalidData` error rather than spin. Drive this by
    /// feeding garbage bytes that don't look like a valid LZ4 frame
    /// header — `FrameDecoder::read` errors out immediately, which
    /// our `?` propagates as an io::Error. This covers the
    /// `consumed == 0` branch's failure mode for any genuinely
    /// stuck decoder.
    #[test]
    fn multi_frame_reader_does_not_spin_on_invalid_bytes() {
        // Bytes that don't start with the LZ4 frame magic
        // (0x184D2204). FrameDecoder will error rather than make
        // progress; our `?` returns it before we ever reach the
        // `consumed == 0` arm. Either outcome (a propagated decode
        // error OR the InvalidData stall path) is acceptable — the
        // contract under test is "doesn't hang."
        let bytes = b"\x00\x01\x02\x03\x04\x05\x06\x07not-a-valid-lz4-frame";
        let mut r = MultiFrameLz4Reader::new(bytes);
        let mut buf = [0u8; 16];
        let err = r
            .read(&mut buf)
            .expect_err("invalid bytes must surface as error");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// Empty input → immediate EOF, no error.
    #[test]
    fn multi_frame_reader_yields_eof_on_empty_input() {
        let mut r = MultiFrameLz4Reader::new(&[]);
        let mut buf = [0u8; 16];
        assert_eq!(r.read(&mut buf).unwrap(), 0, "empty input must EOF cleanly");
    }
}
