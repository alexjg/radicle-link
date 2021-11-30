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
    io::{self, AsyncTransport, SocketTransportError},
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
) -> impl futures::stream::Stream<Item = link_async::Task<anyhow::Result<()>>> + Send + '_
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

async fn rpc<S>(
    spawner: Arc<Spawner>,
    peer: Peer<S>,
    stream: UnixStream,
    announce_wait_time: Duration,
) -> anyhow::Result<()>
where
    S: Signer + Clone,
{
    let mut fire_and_forget_tasks = FuturesUnordered::new();
    let mut transport = io::SocketTransport::new_tokio(stream);
    loop {
        let mut next_ff = fire_and_forget_tasks.next().fuse();
        futures::select! {
            next = transport.recv_request().fuse() => {
                match next {
                    Ok(Some(next)) => {
                        match next.mode {
                            messages::RequestMode::ReportProgress => {
                                let request_id = next.id.clone();
                                let (sx, mut rx) = channel(1);
                                let listener = Listener::Someone(sx);
                                let handler = spawner.spawn(
                                    dispatch_request(
                                        listener,
                                        peer.clone(),
                                        announce_wait_time,
                                        next.payload,
                                    )
                                );
                                while let Some(msg) = rx.recv().await {
                                    transport.send_response(messages::Response{
                                        request_id: request_id.clone(),
                                        payload: msg,
                                    }).await?;
                                }
                                let task_result = handler.await;
                                handle_task_complete::<()>(task_result)?;
                            },
                            messages::RequestMode::FireAndForget => {
                                let handler = spawner.spawn(
                                    dispatch_request(
                                        Listener::NoOne,
                                        peer.clone(),
                                        announce_wait_time,
                                        next.payload,
                                    )
                                );
                                fire_and_forget_tasks.push(handler);
                            }
                        }
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
                                tracing::error!(err=?e, "error receiving message");
                                return Err(e.into())
                            },
                        }
                    }
                }
            },
            finished_f_and_f = next_ff => {
                if let Some(task) = finished_f_and_f {
                    handle_task_complete::<()>(task)?;
                }
            }
        }
    }
    while let Some(f_and_f) = fire_and_forget_tasks.next().await {
        handle_task_complete::<()>(f_and_f)?;
    }
    Ok(())
}

fn handle_task_complete<T>(
    task_result: Result<(), link_async::JoinError>,
) -> Result<(), anyhow::Error> {
    match task_result {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.is_panic() {
                panic::resume_unwind(e.into_panic());
            } else {
                tracing::warn!("task unexpectedly cancelled");
                Ok(())
            }
        },
    }
}

enum Listener {
    NoOne,
    Someone(Sender<messages::ResponsePayload>),
}

impl Listener {
    async fn error(&mut self, error: String) {
        self.send(messages::ResponsePayload::Error(error)).await
    }

    async fn progress(&mut self, message: String) {
        self.send(messages::ResponsePayload::Progress(message))
            .await
    }

    async fn success(&mut self) {
        self.send(messages::ResponsePayload::Success).await
    }

    async fn send(&mut self, msg: messages::ResponsePayload) {
        match self {
            Listener::Someone(sx) => {
                // SAFETY: If we get an error here it's because we're not reading from the
                // receiver end. Crashing is probably the best thing we can do
                // in this case
                match sx.send(msg).await {
                    Ok(()) => {},
                    Err(_) => panic!("unable to send response"),
                }
            },
            Listener::NoOne => {},
        }
    }
}

async fn dispatch_request<S>(
    listener: Listener,
    peer: Peer<S>,
    announce_wait_time: Duration,
    payload: messages::RequestPayload,
) where
    S: Signer + Clone,
{
    tracing::info!(?payload, "dispatching request");
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
