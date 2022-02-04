// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use futures::{future::FutureExt, stream::FuturesUnordered};
use std::{panic, sync::Arc, time::Duration};

use futures::stream::StreamExt;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::mpsc::{channel, Sender},
};

use super::{
    io::{self, SocketTransportError, Transport},
    messages,
};

use librad::{
    git::Urn,
    net::{peer::Peer, protocol::gossip},
    PeerId,
    Signer,
};
use link_async::{incoming::UnixListenerExt, Spawner};

pub fn tasks<S>(
    spawner: Arc<Spawner>,
    peer: Peer<S>,
    socket: &UnixListener,
    announce_wait_time: Duration,
) -> impl futures::stream::Stream<Item = link_async::Task<()>> + Send + '_
where
    S: Signer + Clone,
{
    socket
        .incoming()
        .map(move |stream| match stream {
            Ok(stream) => {
                tracing::debug!("new connection");
                Some(spawner.spawn(rpc(
                    spawner.clone(),
                    peer.clone(),
                    stream,
                    announce_wait_time,
                )))
            },
            Err(e) => {
                tracing::error!(err=?e, "error accepting connection");
                None
            },
        })
        .take_while(|e| futures::future::ready(e.is_some()))
        .filter_map(futures::future::ready)
}

const MAX_IN_FLIGHT_REQUESTS: usize = 20;

async fn rpc<S>(
    spawner: Arc<Spawner>,
    peer: Peer<S>,
    stream: UnixStream,
    announce_wait_time: Duration,
) where
    S: Signer + Clone,
{
    let mut running_handlers = FuturesUnordered::new();
    let mut transport: io::SocketTransport = stream.into();
    // TODO: What should the buffer size be here?
    let (resp_sx, mut resp_rx) = channel(10);
    loop {
        let next = if running_handlers.len() < MAX_IN_FLIGHT_REQUESTS {
            transport.recv_request()
        } else {
            futures::future::pending().boxed()
        };
        let mut next_complete = running_handlers.next().fuse();
        futures::select! {
            next = next.fuse() => {
                match next {
                    Ok(Some(next)) => {
                        let listener = match next.mode {
                            messages::RequestMode::ReportProgress => Listener::progress_and_result(resp_sx.clone()),
                            messages::RequestMode::FireAndForget => Listener::ackonly(resp_sx.clone()),
                        };
                        let handler = spawner.spawn(
                            dispatch_request(
                                listener,
                                peer.clone(),
                                announce_wait_time,
                                next.payload,
                            )
                        );
                        running_handlers.push(handler);
                    },
                    Ok(None) => {
                        tracing::info!("closing connection");
                        break;
                    },
                    Err(e) => {
                        match e {
                            SocketTransportError::DecodeFailed => {
                                tracing::error!(err=?e, "failed to decode message, ignoring");
                            },
                            e => {
                                tracing::error!(err=?e, "error receiving message, closing");
                                break;
                            },
                        }
                    }
                }
            },
            next_complete = next_complete => {
                if let Some(task) = next_complete {
                    handle_task_complete::<()>(task);
                }
            },
            response = resp_rx.recv().fuse() => {
                match response {
                    Some(response) => {
                        match transport.send_response(response).await {
                            Ok(()) => {},
                            Err(e) => {
                                tracing::error!(err=?e, "error sending response");
                            }
                        }
                    },
                    None => {
                        tracing::error!("response channel closed");
                        break;
                    }
                }
            }
        }
    }
    while let Some(complete) = running_handlers.next().await {
        handle_task_complete::<()>(complete);
    }
}

fn handle_task_complete<T>(task_result: Result<(), link_async::JoinError>) {
    match task_result {
        Ok(_) => (),
        Err(e) => {
            if e.is_panic() {
                panic::resume_unwind(e.into_panic());
            } else {
                tracing::warn!("task unexpectedly cancelled");
            }
        },
    }
}

struct Listener {
    request_id: messages::RequestId,
    send: Sender<messages::Response>,
    interest: ListenerInterest,
}

impl Listener {
    fn ackonly(send: Sender<messages::Response>) -> Self {
        Listener {
            request_id: Default::default(),
            interest: ListenerInterest::AckOnly,
            send,
        }
    }

    fn progress_and_result(send: Sender<messages::Response>) -> Self {
        Listener {
            request_id: Default::default(),
            interest: ListenerInterest::ProgressAndResult,
            send,
        }
    }
}

enum ListenerInterest {
    AckOnly,
    ProgressAndResult,
}

impl Listener {
    async fn ack(&mut self) {
        self.send(messages::ResponsePayload::Ack).await
    }

    async fn error(&mut self, error: String) {
        match self.interest {
            ListenerInterest::AckOnly => {},
            ListenerInterest::ProgressAndResult => {
                self.send(messages::ResponsePayload::Error(error)).await
            },
        }
    }

    async fn progress(&mut self, message: String) {
        match self.interest {
            ListenerInterest::AckOnly => {},
            ListenerInterest::ProgressAndResult => {
                self.send(messages::ResponsePayload::Progress(message))
                    .await
            },
        }
    }

    async fn success(&mut self) {
        match self.interest {
            ListenerInterest::AckOnly => {},
            ListenerInterest::ProgressAndResult => {
                self.send(messages::ResponsePayload::Success).await
            },
        }
    }

    async fn send(&mut self, msg: messages::ResponsePayload) {
        let resp = messages::Response {
            request_id: self.request_id.clone(),
            payload: msg,
        };
        match self.send.send(resp).await {
            Ok(()) => {},
            Err(_) => {
                tracing::error!("error sending response");
            },
        }
    }
}

async fn dispatch_request<S>(
    mut listener: Listener,
    peer: Peer<S>,
    announce_wait_time: Duration,
    payload: messages::RequestPayload,
) where
    S: Signer + Clone,
{
    tracing::info!(?payload, "dispatching request");
    listener.ack().await;
    match payload {
        messages::RequestPayload::Announce { urn, rev } => {
            handle_announce(listener, &peer, announce_wait_time, urn, rev).await
        },
    }
}

#[tracing::instrument(skip(listener, peer))]
async fn handle_announce<S>(
    mut listener: Listener,
    peer: &Peer<S>,
    announce_wait_time: Duration,
    urn: Urn,
    rev: git2::Oid,
) where
    S: Signer + Clone,
{
    tracing::info!(?rev, ?urn, "received announce request");
    let gossip_announce = mk_gossip(peer.peer_id(), &urn, &rev);
    if peer.connected_peers().await.is_empty() {
        tracing::debug!(wait_time=?announce_wait_time, "No connected peers, waiting a bit");
        listener
            .progress(format!(
                "no connected peers, waiting {} seconds",
                announce_wait_time.as_secs()
            ))
            .await;
        link_async::sleep(announce_wait_time).await;
    }
    let num_connected = peer.connected_peers().await.len();
    listener
        .progress(format!("found {} peers", num_connected))
        .await;
    if peer.announce(gossip_announce).is_err() {
        // This error can occur if there are no recievers in the running peer to handle
        // the announcement message.
        tracing::error!("failed to send message to announcement subroutine");
        listener.error("unable to announce".to_string()).await;
    } else {
        listener.success().await;
    }
}

fn mk_gossip(peer_id: PeerId, urn: &Urn, revision: &git2::Oid) -> gossip::Payload {
    gossip::Payload {
        urn: urn.clone(),
        rev: Some((*revision).into()),
        origin: Some(peer_id),
    }
}
