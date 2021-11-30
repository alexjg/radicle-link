// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::path::PathBuf;

use librad::profile::Profile;
use tokio::net::UnixListener;

#[cfg(unix)]
pub mod socket_activation;

enum OpenMode {
    /// File descriptors were provided by socket activation
    SocketActivated,
    /// File descriptors were created by this process
    InProcess {
        event_socket_path: PathBuf,
        api_socket_path: PathBuf,
    },
}

/// Sockets the RPC and events APIs will listen on
pub struct Sockets {
    api: UnixListener,
    events: UnixListener,
    open_mode: OpenMode,
}

impl Sockets {
    /// The socket applications will connect to the API over
    pub fn api(&self) -> &UnixListener {
        &self.api
    }

    /// The socket applications will publish and consume events over
    pub fn events(&self) -> &UnixListener {
        &self.events
    }

    /// Perform any cleanup necessary once you're finished with the sockets
    ///
    /// If the process is socket activated this won't do anything. Otherwise
    /// this will remove the socket files which were created when the
    /// sockets were loaded.
    pub fn cleanup(&self) -> std::io::Result<()> {
        match &self.open_mode {
            // Do nothing, the file descriptors are cleaned up by the activation framework
            OpenMode::SocketActivated => {},
            // We must remove these as we created them
            OpenMode::InProcess {
                event_socket_path,
                api_socket_path,
            } => {
                std::fs::remove_file(event_socket_path)?;
                std::fs::remove_file(api_socket_path)?;
            },
        }
        Ok(())
    }
}

impl Sockets {
    pub fn load(profile: &Profile) -> anyhow::Result<Sockets> {
        if let Some(s) = socket_activation::env()? {
            tracing::info!("using sockets specified in socket activation environment variables");
            Ok(s)
        } else {
            tracing::info!("using sockets in default path locations");
            socket_activation::profile(profile)
        }
    }
}
