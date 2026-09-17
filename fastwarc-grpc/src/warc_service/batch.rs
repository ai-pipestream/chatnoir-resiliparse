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

//! Batching of `ParseWarc` response events into `RecordBatch` messages.

use prost::Message;

use super::ResponseSender;
use crate::proto::fastwarc::v1 as pb;

/// Upper bound (2 MiB) on the encoded size of one batch. Keeps batched
/// responses well below the transport's 16 MiB message limit
/// ([`crate::transport::MAX_MESSAGE_SIZE`]) regardless of the configured
/// batch size.
const MAX_BATCH_BYTES: usize = 2 << 20;
/// Field number of `RecordBatch.items`, used to size each item's key and
/// length prefix when tracking the encoded batch size.
const ITEM_TAG: u32 = 1;

/// Groups protocol events into gRPC batch messages.
///
/// The parser emits one event at a time; this emitter buffers up to
/// `batch_size` events (the config's `response_batch_size`) or
/// [`MAX_BATCH_BYTES`] encoded bytes, whichever fills first, and sends them as
/// one `RecordBatch` response. With a batch size of one it degenerates to
/// pass-through sending. Events are never reordered, and the caller must
/// invoke [`BatchEmitter::flush`] after the last event to deliver a partially
/// filled batch.
pub(super) struct BatchEmitter<'a> {
    /// Response stream that receives the finished batches.
    tx: &'a ResponseSender,
    /// Events accumulated for the current batch, in emission order.
    batch: Vec<pb::ParseWarcResponse>,
    /// Number of events that triggers a flush; `<= 1` disables batching.
    batch_size: usize,
    /// Encoded size of `batch` as `RecordBatch.items` entries, including each
    /// item's key and length prefix.
    batch_bytes: usize,
}

impl BatchEmitter<'_> {
    /// An emitter that groups `batch_size` events per response on `tx`.
    pub(super) fn new(tx: &ResponseSender, batch_size: usize) -> BatchEmitter<'_> {
        BatchEmitter {
            tx,
            batch: Vec::new(),
            batch_size,
            batch_bytes: 0,
        }
    }

    /// Queues one event, sending a batch whenever a limit is reached.
    ///
    /// An event that would push the pending batch past [`MAX_BATCH_BYTES`]
    /// flushes the batch first and starts the next one, so no batch exceeds
    /// the byte limit by more than a single event. Returns `false` once the
    /// response channel is closed, signalling the parser to stop.
    pub(super) fn emit(&mut self, response: pb::ParseWarcResponse) -> bool {
        if self.batch_size <= 1 {
            return send_response(self.tx, response);
        }

        let encoded_len = response.encoded_len();
        let response_bytes = encoded_len
            .saturating_add(prost::encoding::key_len(ITEM_TAG))
            .saturating_add(prost::encoding::encoded_len_varint(encoded_len as u64));
        if !self.batch.is_empty() && self.batch_bytes.saturating_add(response_bytes) > MAX_BATCH_BYTES && !self.flush()
        {
            return false;
        }

        self.batch_bytes = self.batch_bytes.saturating_add(response_bytes);
        self.batch.push(response);
        if self.batch.len() >= self.batch_size || self.batch_bytes >= MAX_BATCH_BYTES {
            self.flush()
        } else {
            true
        }
    }

    /// Sends any pending events and resets the batch.
    ///
    /// A single pending event is sent unwrapped rather than as a one-item
    /// batch. Returns `true` when nothing was pending or the send succeeded,
    /// `false` when the response channel is closed.
    pub(super) fn flush(&mut self) -> bool {
        if self.batch.is_empty() {
            return true;
        }

        self.batch_bytes = 0;
        let items = std::mem::take(&mut self.batch);
        let response = if items.len() == 1 {
            items.into_iter().next().expect("checked non-empty")
        } else {
            pb::ParseWarcResponse {
                kind: Some(pb::parse_warc_response::Kind::Batch(pb::RecordBatch { items })),
            }
        };
        send_response(self.tx, response)
    }
}

/// Sends one response from the blocking parser thread.
///
/// Tries a non-blocking send first and only falls back to `blocking_send`
/// when the channel is full, so the parser blocks on the consumer only when
/// the response buffer is exhausted. Returns `false` when the receiver is
/// gone (client disconnected or stream dropped).
fn send_response(tx: &ResponseSender, response: pb::ParseWarcResponse) -> bool {
    match tx.try_send(Ok(response)) {
        Ok(()) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Full(message)) => tx.blocking_send(message).is_ok(),
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
    }
}
