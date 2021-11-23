// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use anyhow::Result;
use nix::{sys::socket, unistd::Pid};
use std::{fs::remove_file, os::unix::process::CommandExt as _, process::Command};

fn main() -> Result<()> {
    make_sock("api")?;
    make_sock("events")?;

    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("-p")
        .arg("radicle-link-test")
        .arg("--example")
        .arg("socket_activation");
    cmd.env("LISTEN_FDS", "2");
    cmd.env("LISTEN_FDNAMES", "api:events");
    cmd.env("LISTEN_PID", Pid::this().to_string());
    cmd.exec();

    Ok(())
}

fn make_sock(name: &str) -> Result<()> {
    let sock_name = format!("/tmp/test-linkd-socket-activation-{}.sock", name);
    remove_file(sock_name.as_str()).ok();

    let sock = socket::socket(
        socket::AddressFamily::Unix,
        socket::SockType::Stream,
        socket::SockFlag::empty(),
        None,
    )?;
    let addr = socket::SockAddr::new_unix(sock_name.as_str())?;
    socket::bind(sock, &addr)?;
    socket::listen(sock, 1)?;
    Ok(())
}
