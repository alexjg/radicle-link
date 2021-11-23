// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use tokio::net::UnixListener;

use anyhow::Result;

#[cfg(all(unix, target_os = "macos"))]
mod macos;
#[cfg(all(unix, target_os = "macos"))]
use macos as imp;

#[cfg(all(unix, not(target_os = "macos")))]
mod unix;
#[cfg(all(unix, not(target_os = "macos")))]
use unix as imp;

/// Sockets used to activate the service
pub struct Sockets {
    /// The socket applications will connect to the API over
    pub api: UnixListener,
    /// The socket applications will publish and consume events over
    pub events: UnixListener,
}

/// Constructs a `Sockets` from the file descriptors passed through the
/// environemnt. The result will be `None` if there are no environment variables
/// set that are applicable for the current platform or no suitable
/// implementations are activated/supported:
///
/// * [systemd] under unix systems with an OS other than macos
/// * [launchd] under macos
///
/// [systemd]: https://www.freedesktop.org/software/systemd/man/systemd.socket.html
/// [launchd]: https://en.wikipedia.org/wiki/Launchd#Socket_activation_protocol
pub fn env() -> Result<Option<Sockets>> {
    imp::env()
}
