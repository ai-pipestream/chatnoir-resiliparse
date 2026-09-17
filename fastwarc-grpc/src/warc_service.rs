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

//! The `fastwarc.v1.WarcService` implementation.
//!
//! [`WarcParser`] implements both RPCs of the service. `ParseWarc` streams
//! archive chunks from the client to a blocking parser task and streams parse
//! events back; `ParseArchive` parses one complete in-message archive and
//! returns the collected records in a single response.
//!
//! The streaming pipeline is split across private submodules:
//!
//! * `channel_reader` adapts the chunk channel fed by the client stream into
//!   a blocking [`std::io::BufRead`] source for the parser.
//! * `parser` runs the [FastWARC](https://docs.rs/fastwarc) iterator over
//!   that source and turns every
//!   record into an ordered `record_start` / `payload_chunk` / `record_end`
//!   (or `record_error`) event sequence.
//! * `batch` optionally packs those events into `RecordBatch` messages to
//!   reduce per-message gRPC overhead before they reach the response stream.

mod batch;
mod channel_reader;
mod parser;

use std::io;

use fastwarc::stream_io::bufread::RawReaderAdapter;
use prost::bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use self::batch::BatchEmitter;
use self::channel_reader::ChannelReader;
use self::parser::{parse_into, record_error};
use crate::convert;
use crate::defaults::{DEFAULT_INPUT_BUFFER_SIZE, MAX_HEADER_LEN, MAX_INPUT_BUFFER_SIZE, MAX_PAYLOAD_CHUNK_SIZE};
use crate::proto::fastwarc::v1 as pb;

/// Capacity of the client-chunk channel. Bounds how far the request reader
/// can run ahead of the parser, providing backpressure to the client.
const CHUNK_CHANNEL_BOUND: usize = 8;
/// Capacity of the response channel. Bounds how many unsent responses the
/// parser can produce before it blocks on the consumer.
const RESPONSE_CHANNEL_BOUND: usize = 8;

/// Sending half of the `ParseWarc` response stream, shared by the parser task
/// and the request forwarder so either side can report a terminal `Status`.
type ResponseSender = mpsc::Sender<Result<pb::ParseWarcResponse, Status>>;

/// Where the parser task reads archive bytes from.
enum ParserInput {
    /// Chunks streamed by the client, delivered through a bounded channel.
    Chunks(mpsc::Receiver<Bytes>),
    /// A file on the server, opened from the config's `archive_path`.
    LocalFile,
}

/// The `fastwarc.v1.WarcService` gRPC service.
///
/// Construct it with [`WarcParser::new`] (the safe default) or
/// [`WarcParser::with_local_files`], then mount it in a tonic server through
/// [`WarcServiceServer`](pb::warc_service_server::WarcServiceServer). See the
/// [crate-level documentation](crate) for a complete embedding example.
#[derive(Default)]
pub struct WarcParser {
    /// Whether `ParseWarc` requests may set `archive_path` to make the server
    /// open a file from its own filesystem.
    allow_local_files: bool,
}

impl WarcParser {
    /// A parser that rejects server-side archive paths.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A parser that permits server-side paths. Only use this when every
    /// client is trusted with read access to the server's files.
    #[must_use]
    pub fn with_local_files() -> Self {
        Self {
            allow_local_files: true,
        }
    }
}

#[tonic::async_trait]
impl pb::warc_service_server::WarcService for WarcParser {
    type ParseWarcStream = ReceiverStream<Result<pb::ParseWarcResponse, Status>>;

    /// Handles the bidirectional streaming `ParseWarc` RPC.
    ///
    /// Reads and validates the leading `config` message, then wires up the
    /// pipeline before returning: a blocking parser task consumes archive
    /// bytes (either the client's `chunk` messages forwarded through a
    /// bounded channel, or a local file when `archive_path` is set and
    /// permitted) and emits parse events into the returned response stream.
    /// Parsing proceeds concurrently with the client upload; this method
    /// itself only performs the setup.
    async fn parse_warc(
        &self,
        request: Request<Streaming<pb::ParseWarcRequest>>,
    ) -> Result<Response<Self::ParseWarcStream>, Status> {
        let mut stream = request.into_inner();
        let config = read_config(&mut stream).await?;
        validate_config(&config)?;
        if !config.archive_path.is_empty() && !self.allow_local_files {
            return Err(Status::permission_denied(
                "archive_path is disabled on this server; stream the archive as chunks instead",
            ));
        }

        let (response_tx, response_rx) = mpsc::channel(RESPONSE_CHANNEL_BOUND);
        if config.archive_path.is_empty() {
            let (chunk_tx, chunk_rx) = mpsc::channel::<Bytes>(CHUNK_CHANNEL_BOUND);
            spawn_parser(ParserInput::Chunks(chunk_rx), response_tx.clone(), config);
            tokio::spawn(forward_chunks(stream, chunk_tx, response_tx));
        } else {
            spawn_parser(ParserInput::LocalFile, response_tx, config);
        }
        Ok(Response::new(ReceiverStream::new(response_rx)))
    }

    /// Handles the unary `ParseArchive` RPC.
    ///
    /// Parses the complete in-message archive on a blocking task and returns
    /// all records and record-level errors in one response. `archive_path` is
    /// rejected here; server-side files are only supported by `ParseWarc`.
    async fn parse_archive(
        &self,
        request: Request<pb::ParseArchiveRequest>,
    ) -> Result<Response<pb::ParseArchiveResponse>, Status> {
        let request = request.into_inner();
        let Some(config) = request.config else {
            return Err(Status::invalid_argument("ParseArchive request must set `config`"));
        };
        validate_config(&config)?;
        if !config.archive_path.is_empty() {
            return Err(Status::invalid_argument("archive_path is only supported by ParseWarc"));
        }

        let archive = request.archive;
        let joined = tokio::task::spawn_blocking(move || collect_archive(io::Cursor::new(archive), &config)).await;
        match joined {
            Ok(response) => Ok(Response::new(response)),
            Err(error) if error.is_panic() => Err(Status::internal("WARC parser task panicked")),
            Err(_) => Err(Status::cancelled("WARC parser task cancelled")),
        }
    }
}

/// Reads the mandatory leading `config` message from the request stream.
///
/// Returns `InvalidArgument` when the stream is empty or its first message
/// carries anything other than a `config`.
async fn read_config(stream: &mut Streaming<pb::ParseWarcRequest>) -> Result<pb::ParseWarcConfig, Status> {
    match stream.message().await? {
        Some(pb::ParseWarcRequest {
            kind: Some(pb::parse_warc_request::Kind::Config(config)),
        }) => Ok(config),
        Some(_) => Err(Status::invalid_argument("first ParseWarc request message must set `config`")),
        None => Err(Status::invalid_argument("empty ParseWarc request stream; first message must set `config`")),
    }
}

/// Runs [`run_parser`] on a blocking task and watches it for failure.
///
/// The parser performs blocking I/O, so it runs via
/// [`tokio::task::spawn_blocking`]. A supervising async task awaits the join
/// handle and converts a panic or cancellation into a terminal gRPC error on
/// the response stream, instead of silently dropping the stream.
fn spawn_parser(input: ParserInput, response_tx: ResponseSender, config: pb::ParseWarcConfig) {
    let error_tx = response_tx.clone();
    // Report parser task failures as terminal gRPC errors.
    tokio::spawn(async move {
        let joined = tokio::task::spawn_blocking(move || run_parser(input, &response_tx, &config)).await;
        if let Err(error) = joined {
            let status = if error.is_panic() {
                Status::internal("WARC parser task panicked")
            } else {
                Status::cancelled("WARC parser task cancelled")
            };
            let _ = error_tx.send(Err(status)).await;
        }
    });
}

/// Forwards the client's `chunk` messages into the parser's input channel.
///
/// Runs until the client closes its stream (normal end of input), the parser
/// drops its receiver (chunk send fails, e.g. after a fatal parse error), or
/// the client violates the protocol. A duplicate `config` or a message with
/// no `kind` set ends the stream with `InvalidArgument`; transport errors are
/// passed through. Closing the chunk sender signals end-of-archive to the
/// parser.
async fn forward_chunks(
    mut stream: Streaming<pb::ParseWarcRequest>,
    chunk_tx: mpsc::Sender<Bytes>,
    response_tx: ResponseSender,
) {
    loop {
        match stream.message().await {
            Ok(Some(pb::ParseWarcRequest {
                kind: Some(pb::parse_warc_request::Kind::Chunk(chunk)),
            })) => {
                if chunk_tx.send(chunk).await.is_err() {
                    return;
                }
            }
            Ok(Some(pb::ParseWarcRequest {
                kind: Some(pb::parse_warc_request::Kind::Config(_)),
            })) => {
                // Already queued parser events may precede this terminal status.
                let _ = response_tx
                    .send(Err(Status::invalid_argument("`config` may only be set on the first request message")))
                    .await;
                return;
            }
            Ok(Some(pb::ParseWarcRequest { kind: None })) => {
                let _ = response_tx
                    .send(Err(Status::invalid_argument("ParseWarc request message must set `config` or `chunk`")))
                    .await;
                return;
            }
            Ok(None) => return,
            Err(error) => {
                let _ = response_tx.send(Err(error)).await;
                return;
            }
        }
    }
}

/// Checks a client config against the hard limits in [`crate::defaults`].
///
/// Rejects size limits above their caps and inverted content-length bounds
/// with `InvalidArgument` before any parsing starts.
fn validate_config(config: &pb::ParseWarcConfig) -> Result<(), Status> {
    validate_limit("max_header_len", config.max_header_len, MAX_HEADER_LEN)?;
    validate_limit("payload_chunk_size", config.payload_chunk_size, MAX_PAYLOAD_CHUNK_SIZE)?;
    validate_limit("input_buffer_size", config.input_buffer_size, MAX_INPUT_BUFFER_SIZE)?;
    if let (Some(min), Some(max)) = (config.min_content_length, config.max_content_length)
        && min > max
    {
        return Err(Status::invalid_argument("min_content_length must not exceed max_content_length"));
    }
    Ok(())
}

/// Rejects a config value above `max` with an `InvalidArgument` naming the
/// offending field. Zero always passes; it selects the default downstream.
fn validate_limit(name: &str, value: u32, max: usize) -> Result<(), Status> {
    if usize::try_from(value).unwrap_or(usize::MAX) > max {
        Err(Status::invalid_argument(format!("{name} must not exceed {max} bytes")))
    } else {
        Ok(())
    }
}

/// Body of the blocking parser task behind `ParseWarc`.
///
/// Selects the byte source described by `input`, runs [`parse_into`] over it,
/// and sends every event through a [`BatchEmitter`] so responses respect the
/// config's `response_batch_size`. A failure to open a configured
/// `archive_path` is reported as a non-recoverable `record_error` rather than
/// a gRPC status. The final flush delivers any partially filled batch.
fn run_parser(input: ParserInput, response_tx: &ResponseSender, config: &pb::ParseWarcConfig) {
    let mut emitter = BatchEmitter::new(response_tx, convert::response_batch_size(config));
    let mut emit = |response| emitter.emit(response);
    match input {
        ParserInput::Chunks(chunk_rx) => {
            parse_into(RawReaderAdapter::new(ChannelReader::new(chunk_rx)), config, &mut emit);
        }
        ParserInput::LocalFile => {
            let input_buffer_size = if config.input_buffer_size == 0 {
                DEFAULT_INPUT_BUFFER_SIZE
            } else {
                config.input_buffer_size as usize
            };
            match std::fs::File::open(&config.archive_path) {
                Ok(file) => parse_into(io::BufReader::with_capacity(input_buffer_size, file), config, &mut emit),
                Err(error) => {
                    emit(record_error(
                        0,
                        false,
                        format!("failed to open archive_path {}: {error}", config.archive_path),
                    ));
                }
            }
        }
    }
    emitter.flush();
}

/// Parses a complete archive and collects the results for `ParseArchive`.
///
/// Reuses the streaming parser and folds its event sequence back into whole
/// [`pb::ParsedRecord`]s with assembled payloads, so both RPCs share one
/// parsing path.
fn collect_archive(
    reader: impl fastwarc::stream_io::traits::IntoWarcReader,
    config: &pb::ParseWarcConfig,
) -> pb::ParseArchiveResponse {
    let mut records = Vec::new();
    let mut errors = Vec::new();
    let mut open: Option<pb::ParsedRecord> = None;
    let mut emit = |response| {
        fold_response(response, &mut open, &mut records, &mut errors);
        true
    };
    parse_into(reader, config, &mut emit);
    pb::ParseArchiveResponse { records, errors }
}

/// Folds one streaming event into the accumulated `ParseArchive` state.
///
/// `open` holds the record started by the last `record_start`: payload chunks
/// append to it, `record_end` moves it into `records`, and `record_error`
/// discards it and appends to `errors`. Batches are flattened recursively in
/// order.
fn fold_response(
    response: pb::ParseWarcResponse,
    open: &mut Option<pb::ParsedRecord>,
    records: &mut Vec<pb::ParsedRecord>,
    errors: &mut Vec<pb::RecordError>,
) {
    match response.kind {
        Some(pb::parse_warc_response::Kind::RecordStart(start)) => {
            *open = Some(pb::ParsedRecord {
                metadata: start.metadata,
                ..Default::default()
            });
        }
        Some(pb::parse_warc_response::Kind::PayloadChunk(chunk)) => {
            if let Some(record) = open.as_mut() {
                record.payload.extend_from_slice(&chunk.data);
            }
        }
        Some(pb::parse_warc_response::Kind::RecordEnd(_)) => {
            if let Some(record) = open.take() {
                records.push(record);
            }
        }
        Some(pb::parse_warc_response::Kind::RecordError(error)) => {
            open.take();
            errors.push(error);
        }
        Some(pb::parse_warc_response::Kind::Batch(batch)) => {
            for item in batch.items {
                fold_response(item, open, records, errors);
            }
        }
        None => {}
    }
}
