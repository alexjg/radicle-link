// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{panic, sync::Arc, time::Duration};

use futures::{future::FutureExt as _, stream::FuturesUnordered, StreamExt};
use structopt::StructOpt as _;
use tokio::sync::mpsc;
use tracing::info;

use librad::{
    crypto::BoxedSigner,
    net::{discovery, peer::Peer},
};

use crate::{
    announcements,
    api,
    args::Args,
    cfg::{self, Cfg, RunMode},
    logging,
    metrics::graphite,
    protocol,
    signals,
    tracking,
};

/// The amount of time to wait for connections before making any announcements
static STARTUP_DELAY: Duration = Duration::from_secs(5);

pub async fn run() -> anyhow::Result<()> {
    logging::init();

    let spawner = Arc::new(link_async::Spawner::from_current().unwrap());

    let args = Args::from_args();
    let cfg: Cfg<discovery::Static, BoxedSigner> = cfg(&args).await?;

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    let mut signals_task = spawner.spawn(signals::routine(shutdown_tx)).fuse();

    let mut coalesced = FuturesUnordered::new();
    let peer = Peer::new(cfg.peer)?;
    let peer_task = spawner
        .spawn(protocol::routine(peer.clone(), cfg.disco, shutdown_rx))
        .fuse();
    coalesced.push(peer_task);

    if let Some(cfg::Metrics::Graphite(addr)) = cfg.metrics {
        let graphite_task = spawner.spawn(graphite::routine(peer.clone(), addr)).fuse();
        coalesced.push(graphite_task);
    }

    if let Some(tracker) = cfg.tracker {
        let tracking_task = spawner
            .spawn(tracking::routine(peer.clone(), tracker))
            .fuse();
        coalesced.push(tracking_task);
    }

    let (evt_sender, evt_receiver) = tokio::sync::mpsc::unbounded_channel();
    let announcement_task = spawner
        .spawn(announcements::routine(
            peer.clone(),
            STARTUP_DELAY,
            evt_receiver,
        ))
        .fuse();
    coalesced.push(announcement_task);

    let timeout = match cfg.run_mode {
        // We add the startup delay to the linger time because the events task sends any events it
        // receives to the announcements subroutine without blobking. This means that the events
        // task could finish - and therefore the server considered idle - before the STARTUP_DELAY
        // has elapsed and so we could potentially miss an announcement.
        RunMode::Mortal(t) => Some(t + STARTUP_DELAY),
        RunMode::Immortal => None,
    };
    let sockets = api::Sockets::load(&cfg.profile)?;
    let api_routine =
        api::routine(spawner.clone(), peer.clone(), evt_sender, &sockets, timeout).fuse();

    futures::pin_mut!(api_routine);

    info!("starting node");
    futures::select! {
        _ = api_routine => {
            tracing::info!("event loop shutdown");
        },
        res = coalesced.next() => {
            if let Some(Err(e)) = res {
                if e.is_panic() {
                    panic::resume_unwind(e.into_panic());
                }
            }
        },
        _ = signals_task => {
        }
    }

    if let Err(e) = sockets.cleanup() {
        tracing::error!(err=?e, "error cleaning up sockets");
    }

    Ok(())
}

#[cfg(unix)]
async fn cfg(args: &Args) -> anyhow::Result<Cfg<discovery::Static, BoxedSigner>> {
    Ok(Cfg::from_args(args).await?)
}

#[cfg(windows)]
async fn cfg(args: &Args) -> anyhow::Result<Cfg<discovery::Static, BoxedSigner>> {
    unimplemented!("Windows is not supported, contributions are welcome :)")
}
