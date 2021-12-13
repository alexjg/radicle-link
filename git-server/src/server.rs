use std::{path::PathBuf, sync::Arc, panic, time::Duration, io::ErrorKind};

use futures::{FutureExt, Stream, StreamExt};
use tokio::net::{TcpListener, TcpStream};

use librad::{
    git::storage::{Pool, Storage},
    PeerId,
};
use link_async::{Spawner, incoming::TcpListenerExt};

use crate::{processes::Processes, exec_str};

#[derive(Clone)]
pub(crate) struct Server {
    spawner: Arc<Spawner>,
    peer: PeerId,
    storage_pool: Arc<Pool<Storage>>,
    rpc_socket_path: PathBuf,
}

impl Server {
    pub(crate) fn new(
        spawner: Arc<Spawner>,
        peer: PeerId,
        storage_pool: Arc<Pool<Storage>>,
        rpc_socket_path: PathBuf,
    ) -> Self {
        Self {
            spawner,
            peer,
            storage_pool,
            rpc_socket_path,
        }
    }

    pub(crate) async fn serve(
        self,
        socket: &TcpListener,
        conf: Arc<thrussh::server::Config>,
    ) -> impl Stream<Item = link_async::Task<Result<(), anyhow::Error>>> + '_ {
        let incoming = socket.incoming();
        incoming
            .map(move |stream| match stream {
                Ok(stream) => Some(run_stream(
                    conf.clone(),
                    self.spawner.clone(),
                    self.peer.clone(),
                    stream,
                    self.storage_pool.clone(),
                    self.rpc_socket_path.clone(),
                )),
                Err(_) => None,
            })
            .take_while(|e| futures::future::ready(e.is_some()))
            .filter_map(futures::future::ready)
    }
}

fn run_stream(
    conf: Arc<thrussh::server::Config>,
    spawner: Arc<link_async::Spawner>,
    peer: librad::PeerId,
    stream: TcpStream,
    pool: Arc<librad::git::storage::Pool<librad::git::storage::Storage>>,
    rpc_socket_path: PathBuf,
) -> link_async::Task<Result<(), anyhow::Error>> {
    let inner_spawner = spawner.clone();
    spawner.spawn(async move {
        let (processes, handle) =
            Processes::new(inner_spawner.clone(), pool.clone(), rpc_socket_path);
        let handler_stream = thrussh::server::run_stream(
            conf.clone(),
            stream,
            SshHandler {
                peer: peer.clone(),
                handle: handle.clone(),
            },
        );
        let mut processes_task = inner_spawner.spawn(processes.run()).fuse();
        futures::select!{
            handler_result = handler_stream.fuse() => {
                match handler_result {
                    Ok(_) => {
                        tracing::info!("server processes disconnected");
                    },
                    Err(e) if e.is_early_eof() => {
                        tracing::warn!("unexpected EOF");
                    },
                    Err(e) => {
                        panic!("error handling SSH: {}", e);
                    }
                }
            },
            processes_result = processes_task => {
                match processes_result {
                    Ok(Ok(())) => {
                        panic!("processes completed whilst handler is still active");
                    },
                    Ok(Err(e)) => {
                        panic!("processes had an error whilst handler still running: {}", e);
                    },
                    Err(e) => {
                        if e.is_panic() {
                            panic::resume_unwind(e.into_panic());
                        } else {
                            panic!("Processes task cancelled whilst handler running");
                        }
                    }

                }
            },
        };
        // If we're here then the handler processes finished, we panic if the processes thread
        // finished first
        handle.stop().await.unwrap();
        match link_async::timeout(Duration::from_millis(500), processes_task).await {
            Ok(Ok(_)) => {},
            Ok(Err(e)) => {
                panic!("error closing processes: {}", e);
            },
            Err(_) => {
                panic!("processes task didn't finish");
            }
        }
        Ok(())
    })
}

struct SshHandler {
    peer: librad::PeerId,
    handle: crate::processes::ProcessesHandle,
}

#[derive(thiserror::Error, Debug)]
enum HandleError {
    #[error(transparent)]
    Thrussh(#[from] thrussh::Error),
    #[error("failed to exec git: {0}")]
    ExecGit(String),
    #[error("failed to send data to git processes: {0}")]
    SendData(String),
}

impl HandleError {
    fn is_early_eof(&self) -> bool {
        match self {
            Self::Thrussh(thrussh::Error::IO(io)) if io.kind() == ErrorKind::UnexpectedEof => true,
            _ => false,
        }
    }
}

impl thrussh::server::Handler for SshHandler {
    type Error = HandleError;
    type FutureAuth = futures::future::Ready<Result<(Self, thrussh::server::Auth), HandleError>>;
    type FutureUnit = std::pin::Pin<
        Box<
            dyn futures::Future<Output = Result<(Self, thrussh::server::Session), HandleError>>
                + Send
                + 'static,
        >,
    >;
    type FutureBool =
        futures::future::Ready<Result<(Self, thrussh::server::Session, bool), HandleError>>;

    fn finished_auth(self, auth: thrussh::server::Auth) -> Self::FutureAuth {
        futures::future::ready(Ok((self, auth)))
    }

    fn finished_bool(self, b: bool, session: thrussh::server::Session) -> Self::FutureBool {
        futures::future::ready(Ok((self, session, b)))
    }

    fn finished(self, session: thrussh::server::Session) -> Self::FutureUnit {
        futures::future::ready(Ok((self, session))).boxed()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn auth_publickey(
        self,
        _user: &str,
        public_key: &thrussh_keys::key::PublicKey,
    ) -> Self::FutureAuth {
        let thrussh_keys::key::PublicKey::Ed25519(k) = public_key;
        let client_key_bytes: &[u8] = &k.key;
        let peer_key_bytes: &[u8] = self.peer.as_ref();
        let auth = if client_key_bytes == peer_key_bytes {
            thrussh::server::Auth::Accept
        } else {
            thrussh::server::Auth::Reject
        };
        self.finished_auth(auth)
    }

    fn data(
        self,
        channel: thrussh::ChannelId,
        data: &[u8],
        session: thrussh::server::Session,
    ) -> Self::FutureUnit {
        let data_vec = data.to_vec();
        async move {
            match self.handle.send(channel, data_vec).await {
                Ok(_) => Ok((self, session)),
                Err(e) => Err(HandleError::SendData(e.to_string())),
            }
        }
        .boxed()
    }

    fn channel_open_session(
        self,
        _channel: thrussh::ChannelId,
        session: thrussh::server::Session,
    ) -> Self::FutureUnit {
        self.finished(session)
    }

    #[tracing::instrument(level = "debug", skip(self, data, session))]
    fn exec_request(
        self,
        channel: thrussh::ChannelId,
        data: &[u8],
        mut session: thrussh::server::Session,
    ) -> Self::FutureUnit {
        let exec_str = String::from_utf8_lossy(data);
        let (service, urn) = exec_str::parse_exec_str(&exec_str).unwrap();
        tracing::debug!(%service, %urn, "exec_request");

        async move {
            match self
                .handle
                .exec_git(channel, session.handle(), service, urn)
                .await
            {
                Ok(_) => {
                    session.channel_success(channel);
                    Ok((self, session))
                }
                Err(e) => Err(HandleError::ExecGit(e.to_string())),
            }
        }
        .boxed()
    }
}
