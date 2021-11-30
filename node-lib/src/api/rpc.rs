// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{sync::Arc, time::Duration};

use async_compat::CompatExt;
use futures::{stream::StreamExt, SinkExt};
use futures_codec::{FramedRead, FramedWrite};
use tokio::net::{UnixListener, UnixStream};

use librad::{
    git::Urn,
    net::{peer::Peer, protocol::gossip},
    PeerId,
    Signer,
};
use link_async::Spawner;

use super::{
    codec,
    wire_types::rpc::{RequestEnvelope, RequestPayload, ResponseEnvelope, ResponsePayload},
};

pub fn tasks<S>(
    spawner: Arc<Spawner>,
    peer: Peer<S>,
    socket: &UnixListener,
    announce_wait_time: Duration,
) -> impl futures::stream::Stream<Item = link_async::Task<anyhow::Result<()>>> + Send + '_
where
    S: Signer + Clone,
{
    let incoming = Incoming(socket);
    incoming
        .map(move |stream| match stream {
            Ok(stream) => {
                tracing::debug!("new connection");
                Some(spawner.spawn(rpc(peer.clone(), stream, announce_wait_time)))
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
    peer: Peer<S>,
    mut stream: UnixStream,
    announce_wait_time: Duration,
) -> anyhow::Result<()>
where
    S: Signer + Clone,
{
    let (rx, sx) = stream.split();
    let codec = codec::LengthDelimitedCborCodec::<ResponseEnvelope, RequestEnvelope>::new();
    let mut recv = FramedRead::new(rx.compat(), &codec);
    let mut send = FramedWrite::new(sx.compat(), &codec);
    while let Some(next) = recv.next().await {
        match next {
            Err(e) => {
                tracing::warn!(err = ?e, "events recv error");
                break;
            },
            Ok(RequestEnvelope {
                headers,
                payload: Some(RequestPayload::Announce { ref urn, ref rev }),
                ..
            }) => {
                tracing::info!(?rev, ?urn, "received announce request");
                let gossip_announce = mk_gossip(peer.peer_id(), urn, rev);
                if peer.connected_peers().await.is_empty() {
                    tracing::debug!(wait_time=?announce_wait_time, "No connected peers, waiting a bit");
                    link_async::sleep(announce_wait_time).await;
                }
                if peer.announce(gossip_announce).is_err() {
                    // This error can occur if there are no recievers in the running peer to handle
                    // the announcement message. It's not obvious that this should matter to
                    // clients so we don't send an error response back, we just log.
                    tracing::error!("failed to send message to announcement subroutine");
                } else {
                    tracing::debug!("announcement successful");
                }
                let response = ResponseEnvelope {
                    response_headers: headers.request_id.into(),
                    response: Some(ResponsePayload::AnnounceSuccess),
                };
                send.send(response).await?;
            },
            _ => {
                tracing::warn!("Unknown message");
            },
        }
    }
    tracing::info!("connection closing");

    Ok(())
}

// Copied from async_std::os::unix::net::Incoming
struct Incoming<'a>(&'a UnixListener);

impl<'a> futures::stream::Stream for Incoming<'a> {
    type Item = std::io::Result<UnixStream>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.0.poll_accept(cx) {
            std::task::Poll::Ready(Ok((socket, _))) => std::task::Poll::Ready(Some(Ok(socket))),
            std::task::Poll::Ready(Err(e)) => {
                tracing::error!(err=?e, "error accepting socket");
                std::task::Poll::Ready(None)
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

fn mk_gossip(peer_id: PeerId, urn: &Urn, revision: &radicle_git_ext::Oid) -> gossip::Payload {
    gossip::Payload {
        urn: urn.clone(),
        rev: Some((*revision).into()),
        origin: Some(peer_id),
    }
}
