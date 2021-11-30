// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

mod events;
pub mod sockets;

use std::{sync::Arc, time::Duration};

use tracing::instrument;

use librad::{crypto::Signer, net::peer::Peer};
use link_async::Spawner;

pub use sockets::Sockets;

use self::wire_types::events::PostReceive;

pub mod wire_types;

#[instrument(name = "api subroutine", skip(spawner, _peer, evt_sender, sockets))]
pub async fn routine<'a, S>(
    spawner: Arc<Spawner>,
    _peer: Peer<S>,
    evt_sender: tokio::sync::mpsc::UnboundedSender<PostReceive>,
    sockets: &'a Sockets,
    linger_timeout: Option<Duration>,
) -> ()
where
    S: Signer + Clone,
{
    let tasks = Box::pin(events::tasks(spawner, evt_sender, sockets.events()));
    if let Some(timeout) = linger_timeout {
        link_async::tasks::run_until_idle(tasks, timeout).await
    } else {
        link_async::tasks::run_forever(tasks).await
    }
}
