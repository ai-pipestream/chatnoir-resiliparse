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

//! Blocking `Read`/`BufRead` adapter over the client's chunk channel.

use std::io::{self, BufRead, Read, Seek, SeekFrom};

use prost::bytes::{Buf, Bytes, BytesMut};
use tokio::sync::mpsc;

/// Bytes of magic prefix the compression detector inspects. Covers the
/// longest magic among the supported formats (LZ4's four bytes; Gzip and Zstd
/// use fewer).
const MAGIC_LEN: usize = 4;

/// Blocking reader over the chunks received by the streaming RPC.
///
/// The [FastWARC](https://docs.rs/fastwarc) parser is synchronous, so it runs
/// on a blocking task and
/// pulls archive bytes through this adapter, which in turn blocks on the
/// bounded channel fed by [`forward_chunks`](super::forward_chunks). A closed
/// channel reads as end-of-file. The reader tracks the absolute stream
/// position to answer the parser's position queries, but the underlying
/// stream cannot rewind, so [`Seek`] only accepts the current position.
pub(super) struct ChannelReader {
    /// Receiving half of the chunk channel; `None` from it means the client
    /// stream ended.
    rx: mpsc::Receiver<Bytes>,
    /// Unconsumed remainder of the most recently received chunk.
    current: Bytes,
    /// Absolute position in the logical archive stream, i.e. the number of
    /// bytes consumed so far.
    pos: u64,
}

impl ChannelReader {
    /// A reader that yields the bytes arriving on `rx` in order.
    pub(super) fn new(rx: mpsc::Receiver<Bytes>) -> Self {
        Self {
            rx,
            current: Bytes::new(),
            pos: 0,
        }
    }
}

impl Read for ChannelReader {
    /// Copies buffered bytes into `buf`, waiting for a chunk when necessary.
    ///
    /// Returns at most one buffered chunk's worth of data per call and `0`
    /// only for an empty `buf` or at end of input.
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

impl BufRead for ChannelReader {
    /// Exposes the next unconsumed bytes, blocking on the channel when empty.
    ///
    /// Empty chunks are skipped, and an empty slice is returned only at end
    /// of input. At stream position zero the buffer is guaranteed to hold at
    /// least [`MAGIC_LEN`] bytes (unless the stream is shorter), coalescing
    /// fragmented chunks so compression detection sees the whole magic prefix
    /// in one window.
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        while self.current.is_empty() {
            match self.rx.blocking_recv() {
                Some(chunk) if chunk.is_empty() => {}
                Some(chunk) => self.current = chunk,
                None => return Ok(&[]),
            }
        }

        // Compression detection needs the first four bytes in one window.
        if self.pos == 0 && self.current.len() < MAGIC_LEN {
            let mut head = BytesMut::from(&self.current[..]);
            while head.len() < MAGIC_LEN {
                match self.rx.blocking_recv() {
                    Some(chunk) => head.extend_from_slice(&chunk),
                    None => break,
                }
            }
            self.current = head.freeze();
        }
        Ok(&self.current)
    }

    /// Marks up to `amt` buffered bytes as read and advances the stream
    /// position accordingly.
    fn consume(&mut self, amt: usize) {
        let n = amt.min(self.current.len());
        self.current.advance(n);
        self.pos += n as u64;
    }
}

impl Seek for ChannelReader {
    /// Reports the current position; actual repositioning is unsupported.
    ///
    /// The parser queries its position through this trait, so no-op seeks
    /// (`SeekFrom::Current(0)` or a `SeekFrom::Start` at the current
    /// position) succeed. Any seek that would move the stream fails with
    /// [`io::ErrorKind::Unsupported`], since streamed chunks cannot be
    /// replayed.
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Current(0) => Ok(self.pos),
            SeekFrom::Start(p) if p == self.pos => Ok(self.pos),
            _ => Err(io::Error::new(io::ErrorKind::Unsupported, "streamed WARC input does not support repositioning")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_read_does_not_wait_for_input() {
        let (_tx, rx) = mpsc::channel(1);
        let mut reader = ChannelReader::new(rx);
        assert_eq!(reader.read(&mut []).unwrap(), 0);
    }

    #[test]
    fn fragmented_stream_head_is_coalesced_for_detection() {
        let (tx, rx) = mpsc::channel(3);
        tx.try_send(Bytes::from_static(b"\x1f")).unwrap();
        tx.try_send(Bytes::from_static(b"\x8b\x08")).unwrap();
        tx.try_send(Bytes::from_static(b"\x00payload")).unwrap();
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        assert_eq!(reader.fill_buf().unwrap(), b"\x1f\x8b\x08\x00payload");
    }

    #[test]
    fn short_stream_returns_available_prefix_at_eof() {
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(Bytes::from_static(b"WA")).unwrap();
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        assert_eq!(reader.fill_buf().unwrap(), b"WA");
    }

    #[test]
    fn seek_accepts_current_position_only() {
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(Bytes::from_static(b"WARC")).unwrap();
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        assert_eq!(reader.stream_position().unwrap(), 0);
        assert_eq!(reader.seek(SeekFrom::Start(0)).unwrap(), 0);

        let mut prefix = [0; 2];
        reader.read_exact(&mut prefix).unwrap();
        assert_eq!(&prefix, b"WA");
        assert_eq!(reader.stream_position().unwrap(), 2);
        assert_eq!(reader.seek(SeekFrom::Start(2)).unwrap(), 2);
        assert_eq!(reader.seek(SeekFrom::Start(0)).unwrap_err().kind(), io::ErrorKind::Unsupported);
        assert_eq!(reader.seek(SeekFrom::Current(1)).unwrap_err().kind(), io::ErrorKind::Unsupported);
        assert_eq!(reader.seek(SeekFrom::End(0)).unwrap_err().kind(), io::ErrorKind::Unsupported);
    }
}
