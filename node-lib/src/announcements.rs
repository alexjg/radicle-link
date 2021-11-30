// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::time::Duration;

use librad::{net::peer::Peer, Signer};
use tokio::sync::mpsc;
use tracing::instrument;

use crate::api::wire_types::events::PostReceive;

#[instrument(name = "announcements subroutine", skip(peer, events))]
pub async fn routine<S: Signer + Clone>(
    peer: Peer<S>,
    startup_wait: Duration,
    mut events: mpsc::UnboundedReceiver<PostReceive>,
) -> anyhow::Result<()> {
    tracing::info!(?startup_wait, "waiting a bit for peer to connect");
    link_async::sleep(startup_wait).await;
    while let Some(postreceive) = events.recv().await {
        let PostReceive { ref urn, rev } = postreceive;
        tracing::info!(?rev, ?urn, "announcing updated revision");
        match peer.announce(postreceive.into()) {
            Ok(()) => {
                tracing::info!("succesful announcement");
            },
            Err(e) => {
                tracing::error!(err=?e, "error announcing");
            },
        }
    }
    tracing::trace!("event channel closed, shutting down");
    Ok(())
}
