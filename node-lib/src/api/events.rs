// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::sync::Arc;

use async_compat::CompatExt;
use futures::stream::StreamExt;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::mpsc,
};

use librad::net::codec;
use link_async::Spawner;

use super::wire_types::events::{Envelope, Message, PostReceive};

pub fn tasks(
    spawner: Arc<Spawner>,
    event_sender: mpsc::UnboundedSender<PostReceive>,
    socket: &UnixListener,
) -> impl futures::stream::Stream<Item = link_async::Task<anyhow::Result<()>>> + Send + '_ {
    let incoming = Incoming(socket);
    incoming
        .map(move |stream| match stream {
            Ok(stream) => {
                tracing::debug!("new connection");
                Some(spawner.spawn(events(event_sender.clone(), stream)))
            },
            Err(e) => {
                tracing::error!(err=?e, "error accepting connection");
                None
            },
        })
        .take_while(|e| futures::future::ready(e.is_some()))
        .filter_map(futures::future::ready)
}

async fn events(
    event_sender: mpsc::UnboundedSender<PostReceive>,
    stream: UnixStream,
) -> anyhow::Result<()> {
    let mut recv: futures_codec::FramedRead<_, codec::CborCodec<(), Envelope>> =
        futures_codec::FramedRead::new(stream.compat(), codec::CborCodec::new());
    while let Some(next) = recv.next().await {
        match next {
            Err(e) => {
                tracing::warn!(err = ?e, "events recv error");
                break;
            },
            Ok(Envelope {
                message: Some(Message::PostReceive(postreceive)),
                ..
            }) => {
                let PostReceive { ref urn, ref rev } = postreceive;
                tracing::trace!(?rev, ?urn, "received PostRecvieve message");
                if event_sender.send(postreceive).is_err() {
                    tracing::error!("failed to send message to announcement subroutine");
                    break;
                }
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
