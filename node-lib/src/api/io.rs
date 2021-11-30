// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{
    convert::TryInto,
    io::{Read, Write},
};

use async_compat::{Compat, CompatExt};
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{
    messages,
    wire_types::{self, Message},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unexpected EOF")]
    UnexpectedEof,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("error decoding")]
    DecodeFailed,
}

pub struct MessageReader<R> {
    reader: R,
    buffer: Vec<u8>,
    state: ReadState,
}

enum ReadState {
    /// Reading length prefix.
    ReadLen([u8; 4], u8),
    /// Reading CBOR item bytes.
    ReadVal(usize),
}

impl ReadState {
    fn new() -> ReadState {
        ReadState::ReadLen([0; 4], 0)
    }
}

impl<R> MessageReader<R> {
    pub fn new(r: R) -> MessageReader<R> {
        MessageReader {
            state: ReadState::new(),
            reader: r,
            buffer: Vec::with_capacity(10),
        }
    }

    fn parse<'b, E: minicbor::Decode<'b>>(&'b self) -> Result<Message<E>, minicbor::decode::Error> {
        let mut decoder = minicbor::Decoder::new(&self.buffer);
        let headers: E = decoder.decode()?;
        let payload = if decoder.position() < self.buffer.len() {
            Some(self.buffer[decoder.position()..].to_vec())
        } else {
            None
        };
        Ok(Message { headers, payload })
    }
}

impl<R: AsyncRead + Unpin> MessageReader<R> {
    /// Read a length prefixed message
    ///
    /// # Cancellation
    ///
    /// This future is cancel safe. Dropping the future half way through
    /// decoding will save progress in the `MessageReader` so that calling
    /// this method again will restart correctly.
    pub(crate) async fn read_message_async<'b, E: minicbor::Decode<'b>>(
        &'b mut self,
    ) -> Result<Option<Message<E>>, Error> {
        loop {
            match self.state {
                ReadState::ReadLen(buf, 4) => {
                    let len = u32::from_be_bytes(buf) as usize;
                    self.buffer.clear();
                    self.buffer.resize(len, 0u8);
                    self.state = ReadState::ReadVal(0)
                },
                ReadState::ReadLen(ref mut buf, ref mut o) => {
                    let n = self.reader.read(&mut buf[usize::from(*o)..]).await?;
                    if n == 0 {
                        return Ok(None);
                    }
                    *o += n as u8
                },
                ReadState::ReadVal(o) if o >= self.buffer.len() => {
                    self.state = ReadState::new();
                    return self.parse().map_err(|_| Error::DecodeFailed).map(Some);
                },
                ReadState::ReadVal(ref mut o) => {
                    let n = self.reader.read(&mut self.buffer[*o..]).await?;
                    if n == 0 {
                        return Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()));
                    }
                    *o += n
                },
            }
        }
    }
}

pub struct MessageWriter<W> {
    buffer: Vec<u8>,
    writer: W,
}

impl<W> MessageWriter<W> {
    pub fn new(w: W) -> MessageWriter<W> {
        MessageWriter {
            writer: w,
            buffer: Vec::new(),
        }
    }

    fn serialize<E: minicbor::Encode>(&mut self, msg: &Message<E>) {
        self.buffer.resize(4, 0u8);
        // SAFETY: We are writing to an in memory buffer and the only thing that can go
        // wrong is the minicbor::Encode impl for the headers being broken, in
        // which case _shrug_
        minicbor::encode(&msg.headers, &mut self.buffer).unwrap();
        if let Some(payload) = &msg.payload {
            self.buffer.extend(payload);
        }
        let prefix = (self.buffer.len() as u32 - 4).to_be_bytes();
        self.buffer[..4].copy_from_slice(&prefix);
    }
}

impl<W: AsyncWrite + Unpin> MessageWriter<W> {
    /// # Cancellation
    ///
    /// This is not cancel safe. Dropping this future will leave incomplete
    /// messages in the buffer.
    pub(crate) async fn write_message_async<E: minicbor::Encode>(
        &mut self,
        msg: &Message<E>,
    ) -> Result<(), std::io::Error> {
        self.serialize(msg);
        let mut offset_written = 0;
        while offset_written < self.buffer.len() {
            let n = self.writer.write(&self.buffer[offset_written..]).await?;
            if n == 0 {
                return Err(std::io::ErrorKind::WriteZero.into());
            }
            offset_written += n;
        }
        Ok(())
    }
}

impl<W: Write> MessageWriter<W> {
    pub(crate) fn write_message_sync<E: minicbor::Encode>(
        &mut self,
        msg: &Message<E>,
    ) -> Result<(), std::io::Error> {
        self.serialize(msg);
        self.writer.write_all(&self.buffer)?;
        Ok(())
    }
}

impl<R: Read> MessageReader<R> {
    pub(crate) fn read_message_sync<'b, E: minicbor::Decode<'b>>(
        &'b mut self,
    ) -> Result<Option<Message<E>>, Error> {
        loop {
            match self.state {
                ReadState::ReadLen(buf, 4) => {
                    let len = u32::from_be_bytes(buf) as usize;
                    self.buffer.clear();
                    self.buffer.resize(len, 0u8);
                    self.state = ReadState::ReadVal(0)
                },
                ReadState::ReadLen(ref mut buf, ref mut o) => {
                    let n = self.reader.read(&mut buf[usize::from(*o)..])?;
                    if n == 0 {
                        return Ok(None);
                    }
                    *o += n as u8
                },
                ReadState::ReadVal(o) if o >= self.buffer.len() => {
                    self.state = ReadState::new();
                    return self.parse().map_err(|_| Error::DecodeFailed).map(Some);
                },
                ReadState::ReadVal(ref mut o) => {
                    let n = self.reader.read(&mut self.buffer[*o..])?;
                    if n == 0 {
                        return Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()));
                    }
                    *o += n
                },
            }
        }
    }
}

pub trait Transport {
    type Error: std::error::Error;

    /// Send a request to the remote
    fn send_request(&mut self, request: messages::Request) -> Result<(), Self::Error>;

    /// Receive a request messages. A `None` return indicates that the
    /// connection has closed
    fn recv_request(&mut self) -> Result<Option<messages::Request>, Self::Error>;

    /// Send a response message to the remote
    fn send_response(&mut self, response: messages::Response) -> Result<(), Self::Error>;

    /// Receive a message from the remote. A return value of `None` indicates
    /// that the connection has closed
    fn recv_response(&mut self) -> Result<Option<messages::Response>, Self::Error>;
}

#[async_trait]
pub trait AsyncTransport {
    type Error;

    /// Send a request to the remote
    async fn send_request(&mut self, request: messages::Request) -> Result<(), Self::Error>;

    /// Receive a request messages. A `None` return indicates that the
    /// connection has closed
    async fn recv_request(&mut self) -> Result<Option<messages::Request>, Self::Error>;

    /// Send a response message to the remote
    async fn send_response(&mut self, response: messages::Response) -> Result<(), Self::Error>;

    /// Receive a message from the remote. A return value of `None` indicates
    /// that the connection has closed
    async fn recv_response(&mut self) -> Result<Option<messages::Response>, Self::Error>;
}

pub struct SyncSocketTransport {
    socket: std::os::unix::net::UnixStream,
}

impl SyncSocketTransport {
    fn new(s: std::os::unix::net::UnixStream) -> Self {
        Self { socket: s }
    }

    fn writer(&mut self) -> MessageWriter<&mut std::os::unix::net::UnixStream> {
        MessageWriter::new(&mut self.socket)
    }

    fn reader(&mut self) -> MessageReader<&mut std::os::unix::net::UnixStream> {
        MessageReader::new(&mut self.socket)
    }

    pub fn close(&self) -> std::io::Result<()> {
        self.socket.shutdown(std::net::Shutdown::Both)
    }
}

pub struct TokioSocketTransport {
    socket: tokio::net::UnixStream,
}

impl TokioSocketTransport {
    fn new(s: tokio::net::UnixStream) -> Self {
        Self { socket: s }
    }

    fn writer(&mut self) -> MessageWriter<Compat<tokio::net::unix::WriteHalf<'_>>> {
        MessageWriter::new(self.socket.split().1.compat())
    }

    fn reader(&mut self) -> MessageReader<Compat<tokio::net::unix::ReadHalf<'_>>> {
        MessageReader::new(self.socket.split().0.compat())
    }
}

pub struct SocketTransport<S> {
    socket: S,
}

impl From<std::os::unix::net::UnixStream> for SocketTransport<SyncSocketTransport> {
    fn from(s: std::os::unix::net::UnixStream) -> Self {
        Self::new_sync(s)
    }
}

impl From<tokio::net::UnixStream> for SocketTransport<TokioSocketTransport> {
    fn from(s: tokio::net::UnixStream) -> Self {
        Self::new_tokio(s)
    }
}

impl SocketTransport<TokioSocketTransport> {
    pub fn new_tokio(s: tokio::net::UnixStream) -> Self {
        Self {
            socket: TokioSocketTransport::new(s),
        }
    }
}

impl SocketTransport<SyncSocketTransport> {
    pub fn new_sync(s: std::os::unix::net::UnixStream) -> Self {
        Self {
            socket: SyncSocketTransport::new(s),
        }
    }

    pub fn close(&self) -> std::io::Result<()> {
        self.socket.close()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SocketTransportError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("unable to decode message")]
    DecodeFailed,
}

impl<S> SocketTransport<S> {
    fn process_recv_response(
        &mut self,
        msg_result: Result<Option<wire_types::Response>, Error>,
    ) -> Result<Option<messages::Response>, SocketTransportError> {
        let wire_message: Option<wire_types::Response> = msg_result.map_err(|e| {
            tracing::error!(err=?e, "failed to decode wire type");
            SocketTransportError::DecodeFailed
        })?;
        if let Some(wire_message) = wire_message {
            let message: messages::Response = wire_message.try_into().map_err(|e| {
                tracing::error!(err=?e, "failed to decode response from wire type");
                SocketTransportError::DecodeFailed
            })?;
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }

    fn process_recv_request(
        &mut self,
        msg_result: Result<Option<wire_types::Request>, Error>,
    ) -> Result<Option<messages::Request>, SocketTransportError> {
        let wire_message: Option<wire_types::Request> = msg_result.map_err(|e| {
            tracing::error!(err=?e, "failed to decode wire type");
            SocketTransportError::DecodeFailed
        })?;
        if let Some(wire_message) = wire_message {
            let message: messages::Request = wire_message.try_into().map_err(|e| {
                tracing::error!(err=?e, "failed to decode response from wire type");
                SocketTransportError::DecodeFailed
            })?;
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }
}

impl Transport for SocketTransport<SyncSocketTransport> {
    type Error = SocketTransportError;

    fn send_request(&mut self, request: messages::Request) -> Result<(), Self::Error> {
        let wire_message: wire_types::Request = request.into();
        self.socket.writer().write_message_sync(&wire_message)?;
        Ok(())
    }

    fn send_response(&mut self, response: messages::Response) -> Result<(), Self::Error> {
        let wire_message: wire_types::Response = response.into();
        self.socket.writer().write_message_sync(&wire_message)?;
        Ok(())
    }

    fn recv_response(&mut self) -> Result<Option<messages::Response>, Self::Error> {
        let wire_message = self.socket.reader().read_message_sync();
        self.process_recv_response(wire_message)
    }

    fn recv_request(&mut self) -> Result<Option<messages::Request>, Self::Error> {
        let wire_message = self.socket.reader().read_message_sync();
        self.process_recv_request(wire_message)
    }
}

#[async_trait]
impl AsyncTransport for SocketTransport<TokioSocketTransport> {
    type Error = SocketTransportError;

    async fn send_request(&mut self, request: messages::Request) -> Result<(), Self::Error> {
        let wire_message: wire_types::Request = request.into();
        self.socket
            .writer()
            .write_message_async(&wire_message)
            .await?;
        Ok(())
    }

    async fn recv_request(&mut self) -> Result<Option<messages::Request>, Self::Error> {
        let wire_message = self.socket.reader().read_message_async().await;
        self.process_recv_request(wire_message)
    }

    async fn send_response(&mut self, response: messages::Response) -> Result<(), Self::Error> {
        let wire_message: wire_types::Response = response.into();
        self.socket
            .writer()
            .write_message_async(&wire_message)
            .await?;
        Ok(())
    }

    async fn recv_response(&mut self) -> Result<Option<messages::Response>, Self::Error> {
        let wire_message = self.socket.reader().read_message_async().await;
        self.process_recv_response(wire_message)
    }
}
