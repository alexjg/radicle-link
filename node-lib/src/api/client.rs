// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

//! This module is the client interface to the p2p node RPC API. The APIs here
//! are designed to work in both an asynchronous and synchronous context. To
//! start you'll need to create a [`Connection`] by calling either
//! [`Connection::connect`] or [`Connection::connect_async`]. Once you have a
//! connection you then create a command using the `commands::*` functions. A
//! command then has various methods on it which determine exactly how the
//! command should be executed.
//!
//! See the documentation of [`Command`] for more information.

use git_ext::Oid;
use radicle_git_ext as git_ext;

use std::os::unix::net::UnixStream;

use librad::git::Urn;

use super::{io, messages};

pub struct Connection<T> {
    socket: T,
    user_agent: messages::UserAgent,
}

impl<T: io::Transport> Connection<T> {
    pub fn new<UA: ToString, IT: Into<T>>(transport: IT, user_agent: UA) -> Self {
        Connection {
            socket: transport.into(),
            user_agent: user_agent.to_string().into(),
        }
    }
}

impl Connection<io::SocketTransport<io::SyncSocketTransport>> {
    /// Connect to the domain socket given by `socket_path`. The `user_agent`
    /// will be used to identify this client in log messages so it's best to
    /// choose something unique. This method will block until a connection
    /// is made.
    pub fn connect<C: ToString, P: AsRef<std::path::Path>>(
        user_agent: C,
        socket_path: P,
    ) -> Result<Self, std::io::Error> {
        let stream = UnixStream::connect(socket_path)?;
        Ok(Self {
            socket: io::SocketTransport::new_sync(stream),
            user_agent: user_agent.to_string().into(),
        })
    }
}

impl Connection<io::SocketTransport<io::TokioSocketTransport>> {
    /// Asynchronously connect to the domain socket given by `socket_path`. The
    /// `user_agent` will be used to identify this client in log messages so
    /// it's best to choose something unique. This method will block until a
    /// connection is made.
    ///
    /// Note that this expects a tokio runtime to be available and will panic if
    /// there isn't one.
    pub async fn connect_async<C: ToString, P: AsRef<std::path::Path>>(
        user_agent: C,
        socket_path: P,
    ) -> Result<Self, std::io::Error> {
        let stream = tokio::net::UnixStream::connect(socket_path).await?;
        Ok(Self {
            socket: io::SocketTransport::new_tokio(stream),
            user_agent: user_agent.to_string().into(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReplyError<T> {
    #[error(transparent)]
    Transport(T),
    #[error("no reply when one was expected")]
    MissingReply,
}

pub struct Replies<T> {
    conn: Connection<T>,
}

impl<T> Replies<T> {
    fn process_recv<E>(
        self,
        msg: Option<messages::Response>,
    ) -> Result<Reply<T>, (Connection<T>, ReplyError<E>)> {
        match msg {
            None => Err((self.conn, ReplyError::MissingReply)),
            Some(msg) => Ok(match msg.payload {
                messages::ResponsePayload::Progress(s) => Reply::Progress(self, s),
                messages::ResponsePayload::Error(s) => Reply::Error(self.conn, s),
                messages::ResponsePayload::Success => Reply::Success(self.conn),
            }),
        }
    }
}

impl<T: io::Transport> Replies<T> {
    /// Synchronously wait for a message from the server which we expect in
    /// response to a messages. A value of `Ok(Reply<T>)` indicates that we
    /// received a message and you should match on the `Reply` to decide
    /// what to do next. A return value of `(Connection, ReplyError)` indicates
    /// that there was some kind of transport error.
    #[allow(clippy::type_complexity)]
    pub fn next(mut self) -> Result<Reply<T>, (Connection<T>, ReplyError<T::Error>)> {
        match self.conn.socket.recv_response() {
            Err(e) => Err((self.conn, ReplyError::Transport(e))),
            Ok(msg) => self.process_recv(msg),
        }
    }
}

impl<T: io::AsyncTransport> Replies<T> {
    /// Asynchronously wait for a message from the server which we expect in
    /// response to a messages. A value of `Ok(Reply<T>)` indicates that we
    /// received a message and you should match on the `Reply` to decide
    /// what to do next. A return value of `(Connection, ReplyError)` indicates
    /// that there was some kind of transport error.
    pub async fn next_async(mut self) -> Result<Reply<T>, (Connection<T>, ReplyError<T::Error>)> {
        match self.conn.socket.recv_response().await {
            Err(e) => Err((self.conn, ReplyError::Transport(e))),
            Ok(msg) => self.process_recv(msg),
        }
    }
}

/// State of an in progress request which we expect to return a response
pub enum Reply<T> {
    /// The server returned a "progress" message
    Progress(Replies<T>, String),
    /// The server indicated an error, no further messages will be sent
    Error(Connection<T>, String),
    /// The server indiciated that the call was successful, no further messages
    /// will be sent
    Success(Connection<T>),
}

pub struct Command(commands::Command);

impl Command {
    /// Synchronously execute this command and set the request mode to "fire and
    /// forget". This means that the server will not send a response so you
    /// do not need to block and read the response
    pub fn execute<T>(self, conn: &mut Connection<T>) -> Result<(), T::Error>
    where
        T: io::Transport,
    {
        let req = self
            .0
            .request(&conn.user_agent, messages::RequestMode::FireAndForget);
        conn.socket.send_request(req)?;
        Ok(())
    }

    /// Synchronously execute this command and wait for a response. Note that
    /// this consumes `conn`. This is deliberate. A successful request will
    /// return a [`Replies`], which exposes further methods to read
    /// responses from the server. For example:
    ///
    /// ```no_run
    /// use node_lib::api::{io::{SocketTransport, SyncSocketTransport}, client::{Connection, Command, Reply}};
    ///
    /// let conn: Connection<SocketTransport<SyncSocketTransport>> = Connection::connect("some user agent".to_string(), "<somepath>").unwrap();
    /// let command: Command = panic!("somehow create a command");
    /// let mut replies = command.execute_with_reply(conn).unwrap();
    /// let next_conn = loop {
    ///     match replies.next() {
    ///         Ok(Reply::Progress(next_replies, msg)) => {
    ///             println!("{}\n", msg);
    ///             replies = next_replies;
    ///         },
    ///         Ok(Reply::Error(conn, msg)) => {
    ///             println!("some error: {}\n", msg);
    ///             break conn;
    ///         },
    ///         Ok(Reply::Success(conn)) => break conn,
    ///         Err((conn, err)) => {
    ///             println!("transport error: {}\n", err);
    ///             break conn;
    ///         }
    ///     }
    /// };
    /// // Do more things with connection
    /// ```
    pub fn execute_with_reply<T>(self, mut conn: Connection<T>) -> Result<Replies<T>, T::Error>
    where
        T: io::Transport,
    {
        let req = self
            .0
            .request(&conn.user_agent, messages::RequestMode::ReportProgress);
        conn.socket.send_request(req)?;
        Ok(Replies { conn })
    }

    /// Asynchronously execute this command and set the request mode to "fire
    /// and forget". This means that the server will not send a response so
    /// you do not need to block and read the response.
    ///
    /// # Cancellation
    ///
    /// Cancelling may leave unfinished messages on the socket, this future is
    /// therefore not cancel safe.
    pub async fn execute_async<T>(self, conn: &mut Connection<T>) -> Result<(), T::Error>
    where
        T: io::AsyncTransport,
    {
        let req = self
            .0
            .request(&conn.user_agent, messages::RequestMode::FireAndForget);
        conn.socket.send_request(req).await?;
        Ok(())
    }

    /// Asynchronously execute this command and wait for a response. Note that
    /// this consumes `conn`. This is deliberate. A successful request will
    /// return a [`Replies`], which exposes further methods to read
    /// responses from the server. For example:
    ///
    /// ```no_run
    /// # async fn dothings() {
    /// use node_lib::api::{io::{SocketTransport, TokioSocketTransport}, client::{Connection, Command, Reply}};
    ///
    /// let conn: Connection<SocketTransport<TokioSocketTransport>> = Connection::connect_async("some user agent".to_string(), "<somepath>").await.unwrap();
    /// let command: Command = panic!("somehow create a command");
    /// let mut replies = command.execute_with_reply_async(conn).await.unwrap();
    /// let next_conn = loop {
    ///     match replies.next_async().await {
    ///         Ok(Reply::Progress(next_replies, msg)) => {
    ///             println!("{}\n", msg);
    ///             replies = next_replies;
    ///         },
    ///         Ok(Reply::Error(conn, msg)) => {
    ///             println!("some error: {}\n", msg);
    ///             break conn;
    ///         },
    ///         Ok(Reply::Success(conn)) => break conn,
    ///         Err((conn, err)) => {
    ///             println!("transport error: {}\n", err);
    ///             break conn;
    ///         }
    ///     }
    /// };
    /// // Do more things with connection
    /// # }
    /// ```
    ///
    /// # Cancelation
    ///
    /// Cancelling may leave unfinished messages on the socket, this future is
    /// therefore not cancel safe. However, this method also consumes the
    /// connection, it's unlikely therefore that you will be able to create
    /// a situation where the borrow checked will allow you to resume with
    /// the socket in an invalid state.
    pub async fn execute_with_reply_async<T>(
        self,
        mut conn: Connection<T>,
    ) -> Result<Replies<T>, T::Error>
    where
        T: io::AsyncTransport,
    {
        let req = self
            .0
            .request(&conn.user_agent, messages::RequestMode::ReportProgress);
        conn.socket.send_request(req).await?;
        Ok(Replies { conn })
    }

    /// Create a command which announces the given urn at a particular revision
    pub fn announce(urn: Urn, revision: Oid) -> Command {
        Command(commands::Command::Announce { rev: revision, urn })
    }
}

mod commands {
    use super::*;

    pub(super) enum Command {
        Announce { urn: Urn, rev: Oid },
    }

    impl Command {
        pub(super) fn request(
            self,
            user_agent: &messages::UserAgent,
            mode: messages::RequestMode,
        ) -> messages::Request {
            match self {
                Self::Announce { rev, urn } => messages::Request {
                    id: Default::default(),
                    user_agent: user_agent.clone(),
                    mode,
                    payload: messages::RequestPayload::Announce {
                        rev: rev.into(),
                        urn,
                    },
                },
            }
        }
    }
}
