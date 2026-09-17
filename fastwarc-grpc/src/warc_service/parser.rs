// Copyright 2026 Kristian Rickert
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

use std::cell::RefCell;
use std::io::{self, BufRead};
use std::rc::Rc;

use fastwarc::stream_io::traits::IntoWarcReader;
use fastwarc::warc::iter::{ArchiveIterator, ArchiveIteratorOptions};
use fastwarc::warc::record::WarcRecord;
use prost::bytes::Bytes;

use crate::convert;
use crate::defaults::{DEFAULT_MAX_HEADER_LEN, DEFAULT_PAYLOAD_CHUNK_SIZE};
use crate::proto::fastwarc::v1 as pb;

type EmitFn<'a> = &'a mut dyn FnMut(pb::ParseWarcResponse) -> bool;

/// Parses an archive and emits one ordered response sequence per record.
pub(super) fn parse_into(reader: impl IntoWarcReader, config: &pb::ParseWarcConfig, emit: EmitFn<'_>) {
    let chunk_size = if config.payload_chunk_size == 0 {
        DEFAULT_PAYLOAD_CHUNK_SIZE
    } else {
        config.payload_chunk_size as usize
    };
    let max_header_len = if config.max_header_len == 0 {
        DEFAULT_MAX_HEADER_LEN
    } else {
        config.max_header_len as usize
    };
    let options = ArchiveIteratorOptions {
        stream_detect: config.stream_detect.unwrap_or(true),
        // Parse in the filter so HTTP and framing errors remain distinct.
        parse_http: false,
        decode_http_payload: convert::auto_decode(config.decode_http_payload),
        verify_digests: config.verify_digests,
        quirks_mode: config.quirks_mode,
        max_header_len,
        inplace: !convert::include_payload(config) && !config.verify_digests,
    };
    let http_error = RefCell::new(None);
    let iterator = ArchiveIterator::with_options(reader, options).with_filter(|record| {
        // Match ArchiveIterator's HTTP parsing order while preserving header
        // failures as recoverable record errors.
        if convert::parse_http(config)
            && record.is_http()
            && let Err(error) = record.parse_http_with_opts(
                convert::auto_decode(config.decode_http_payload),
                max_header_len,
                config.quirks_mode,
            )
        {
            http_error.replace(Some((record.stream_pos(), format!("failed to parse HTTP headers: {error}"))));
            // Yield once so the error is emitted before iteration resumes.
            return true;
        }
        convert::record_passes_filters(record, config)
    });

    for item in iterator {
        let record = match item {
            Ok(record) => record,
            Err(error) => {
                let _ = emit(record_error(0, false, error.to_string()));
                return;
            }
        };
        if let Some((stream_pos, message)) = http_error.borrow_mut().take() {
            if !emit(record_error(stream_pos, true, message)) {
                return;
            }
            continue;
        }
        if !emit_record(&record, config, chunk_size, emit) {
            return;
        }
    }
}

fn emit_record(
    shared: &Rc<RefCell<WarcRecord>>,
    config: &pb::ParseWarcConfig,
    chunk_size: usize,
    emit: EmitFn<'_>,
) -> bool {
    let mut record = shared.borrow_mut();
    let metadata = convert::record_metadata(&record, convert::include_headers(config));
    if !emit(response(pb::parse_warc_response::Kind::RecordStart(pb::RecordStart {
        metadata: Some(metadata),
    }))) {
        return false;
    }

    let payload_length = if convert::include_payload(config) {
        match stream_payload(&mut record, chunk_size, emit) {
            Ok(len) => len,
            Err((bytes_streamed, error)) => {
                let stream_pos = record.stream_pos();
                // The record sequence is already open, so terminate it: a
                // non-recoverable record_error followed by a record_end whose
                // payload_length covers the bytes streamed before the failure.
                // Clients never see an unterminated record sequence.
                if emit(record_error(stream_pos, false, format!("failed to read record payload: {error}"))) {
                    let _ = emit(response(pb::parse_warc_response::Kind::RecordEnd(pb::RecordEnd {
                        payload_length: bytes_streamed,
                    })));
                }
                return false;
            }
        }
    } else {
        record.content_length()
    };

    emit(response(pb::parse_warc_response::Kind::RecordEnd(pb::RecordEnd { payload_length })))
}

/// Streams payload chunks and returns the total byte count, or the bytes
/// streamed before a failure alongside the error.
fn stream_payload(record: &mut WarcRecord, chunk_size: usize, emit: EmitFn<'_>) -> Result<u64, (u64, io::Error)> {
    let Some(reader) = record.reader_mut() else {
        return Ok(0);
    };
    let mut offset = 0u64;
    loop {
        let window = match reader.fill_buf() {
            Ok(window) => window,
            Err(error) => return Err((offset, error)),
        };
        if window.is_empty() {
            return Ok(offset);
        }
        let n = window.len().min(chunk_size);
        let data = Bytes::copy_from_slice(&window[..n]);
        reader.consume(n);
        if !emit(response(pb::parse_warc_response::Kind::PayloadChunk(pb::PayloadChunk { offset, data }))) {
            return Err((offset, io::Error::new(io::ErrorKind::BrokenPipe, "consumer gone")));
        }
        offset += n as u64;
    }
}

fn response(kind: pb::parse_warc_response::Kind) -> pb::ParseWarcResponse {
    pb::ParseWarcResponse { kind: Some(kind) }
}

pub(super) fn record_error(stream_pos: u64, recoverable: bool, message: String) -> pb::ParseWarcResponse {
    response(pb::parse_warc_response::Kind::RecordError(pb::RecordError {
        stream_pos,
        recoverable,
        message,
    }))
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};

    use fastwarc::stream_io::bufread::RawReaderAdapter;

    use super::*;

    /// Serves a fixed prefix of archive bytes, then fails every later read.
    struct FailingReader {
        data: Vec<u8>,
        pos: usize,
    }

    impl BufRead for FailingReader {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            if self.pos < self.data.len() {
                Ok(&self.data[self.pos..])
            } else {
                Err(io::Error::new(io::ErrorKind::ConnectionReset, "simulated mid-payload failure"))
            }
        }

        fn consume(&mut self, amt: usize) {
            self.pos = (self.pos + amt).min(self.data.len());
        }
    }

    impl Read for FailingReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            let src = self.fill_buf()?;
            let n = src.len().min(buf.len());
            buf[..n].copy_from_slice(&src[..n]);
            self.consume(n);
            Ok(n)
        }
    }

    impl Seek for FailingReader {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            let current = self.pos as u64;
            match pos {
                SeekFrom::Current(0) => Ok(current),
                SeekFrom::Start(p) if p == current => Ok(current),
                _ => Err(io::Error::new(io::ErrorKind::Unsupported, "no repositioning")),
            }
        }
    }

    /// A payload read failure after `record_start` was emitted must terminate
    /// the open sequence: `record_error` followed by a `record_end` whose
    /// `payload_length` counts the chunk bytes streamed before the failure.
    #[test]
    fn payload_failure_after_start_terminates_record_sequence() {
        // A record that declares more payload than the reader can serve: the
        // header and the first 10 payload bytes parse, the next read fails.
        let mut data = Vec::new();
        write!(
            data,
            "WARC/1.0\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:uuid:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa>\r\nWARC-Date: 2020-01-01T00:00:00Z\r\nContent-Length: 1000\r\n\r\n"
        )
        .unwrap();
        data.extend_from_slice(b"0123456789");

        let config = pb::ParseWarcConfig {
            parse_http: Some(false),
            ..Default::default()
        };
        let mut events = Vec::new();
        let mut emit = |response: pb::ParseWarcResponse| {
            events.push(response);
            true
        };
        parse_into(RawReaderAdapter::new(FailingReader { data, pos: 0 }), &config, &mut emit);

        let kinds: Vec<_> = events.iter().map(|e| e.kind.as_ref().unwrap()).collect();
        let [
            pb::parse_warc_response::Kind::RecordStart(_),
            payload_chunks @ ..,
            pb::parse_warc_response::Kind::RecordError(error),
            pb::parse_warc_response::Kind::RecordEnd(end),
        ] = kinds.as_slice()
        else {
            panic!("expected record_start .. record_error record_end, got {kinds:?}");
        };
        let mut streamed = 0u64;
        for kind in payload_chunks {
            let pb::parse_warc_response::Kind::PayloadChunk(chunk) = kind else {
                panic!("expected only payload_chunk between record_start and record_error, got {kind:?}");
            };
            assert_eq!(chunk.offset, streamed, "non-contiguous payload chunk offset");
            streamed += chunk.data.len() as u64;
        }
        assert!(streamed > 0, "the failure should occur after at least one payload chunk");
        assert!(!error.recoverable);
        assert!(error.message.contains("failed to read record payload"), "unexpected message: {}", error.message);
        assert_eq!(end.payload_length, streamed, "record_end must count the bytes streamed before the failure");
    }
}
