// Copyright © 2022 The Radicle Link Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#[macro_use]
extern crate async_trait;

use std::{collections::HashMap, path::PathBuf};

use ::tokio::sync::mpsc;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt as _};
use multihash::Multihash;

use link_identities::urn::{HasProtocol, Urn};

pub mod data;
pub use data::Data;

pub mod track;
pub use track::Track;

pub enum Event {
    Track,
    Data,
}

mod sealed;

pub trait Display: sealed::Sealed {
    fn display(&self) -> String;
}

pub trait IsZero {
    fn is_zero(&self) -> bool;
}

/// End of transimission character.
pub const EOT: u8 = 0x04;

#[cfg(feature = "git")]
mod git {
    use git2::Oid;
    use radicle_git_ext as ext;

    use super::IsZero;

    impl IsZero for Oid {
        fn is_zero(&self) -> bool {
            self == &Oid::zero()
        }
    }

    impl IsZero for ext::Oid {
        fn is_zero(&self) -> bool {
            git2::Oid::from(*self).is_zero()
        }
    }
}

/// This is not cancel safe
#[async_trait]
pub trait Handle: Sized {
    type SpawnError: std::error::Error + Send + Sync + 'static;
    type WriteError: std::error::Error + Send + Sync + 'static;

    async fn spawn(path: PathBuf) -> Result<Self, Self::SpawnError>;
    async fn write(&mut self, bs: &[u8]) -> Result<(), Self::WriteError>;
    // async fn eot(&mut self) -> Result<(), Self::EOTError>;
}

// pub trait Hook {
//     type Handle: Handle;
//     type HandleError: std::error::Error + Send + Sync + 'static;

//     fn handle(&self) -> Result<Self::Handle, Self::HandleError>;
// }

// TODO(finto): We may want to be able to interleave notifying new updates until
// the EOT is sent. For example, track peer A, send notification, do more
// things, track peer B, send notification, send EOT.

// TODO(finto): handle and write would probably need to be async to allow for
// interleaving.

pub enum Notification<R> {
    Track(Track<R>),
    Data(Data<R>),
}

pub struct Hook<P: Handle> {
    path: PathBuf,
    child: P,
}

impl<P: Handle + Send + Sync + 'static> Hook<P> {
    pub fn new(path: PathBuf, child: P) -> Self {
        Self { path, child }
    }

    pub fn start<'a, D>(mut self) -> (mpsc::Sender<D>, futures::future::BoxFuture<'a, PathBuf>)
    where
        D: Display + Send + Sync + 'static,
    {
        // TODO: figure out why this is 10
        let (sx, mut rx) = mpsc::channel::<D>(10);
        let routine = async move {
            while let Some(msg) = rx.recv().await {
                if let Err(err) = self.write(msg.display().as_bytes()).await {
                    tracing::warn!(path = %self.path.display(), err = %err, "failed to write to hook");
                    return self.path;
                }
            }
            self.path
        }.boxed();
        (sx, routine)
    }
}

#[async_trait]
impl<P> Handle for Hook<P>
where
    P: Handle + Send + Sync + 'static,
{
    type WriteError = P::WriteError;
    type SpawnError = P::SpawnError;

    async fn spawn(path: PathBuf) -> Result<Self, Self::SpawnError> {
        Ok(Self {
            path: path.clone(),
            child: P::spawn(path).await?,
        })
    }

    async fn write(&mut self, bs: &[u8]) -> Result<(), Self::WriteError> {
        self.child.write(bs).await
    }
}

pub mod tokio {
    use std::{io, path::PathBuf, process::Stdio};
    use tokio::{
        io::AsyncWriteExt,
        process::{Child, Command},
    };

    use super::{Handle, Hook};

    pub fn hook(path: PathBuf) -> io::Result<Hook<Child>> {
        let child = Command::new(path.clone())
            .stdin(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        Ok(Hook::new(path, child))
    }

    #[async_trait]
    impl Handle for Child {
        type WriteError = io::Error;
        type SpawnError = io::Error;

        async fn spawn(path: PathBuf) -> Result<Self, Self::SpawnError> {
            let child = Command::new(path.clone()).stdin(Stdio::piped()).spawn()?;
            Ok(child)
        }

        async fn write(&mut self, bs: &[u8]) -> Result<(), Self::WriteError> {
            self.stdin
                .as_mut()
                .expect("BUG: stdin was not set up for subprocess")
                .write_all(bs)
                .await
        }
    }
}

struct Hooks<P: Handle, R> {
    rx: ::tokio::sync::mpsc::Receiver<Notification<R>>,
    data_hooks: Vec<Hook<P>>,
    track_hooks: Vec<Hook<P>>,
}

impl<P: Handle + Send + Sync + 'static, R> Hooks<P, R>
where
    R: Clone + HasProtocol + std::fmt::Display + Send + Sync + 'static,
    for<'a> &'a R: Into<Multihash>,
{
    async fn run(mut self, stop: ::tokio::sync::oneshot::Receiver<()>) {
        let mut routines = FuturesUnordered::new();
        let mut data_senders: HashMap<PathBuf, mpsc::Sender<Data<R>>> = HashMap::new();
        let mut track_senders: HashMap<PathBuf, mpsc::Sender<Track<R>>> = HashMap::new();

        for hook in self.data_hooks {
            let path = hook.path.clone();
            let (sender, routine) = hook.start();
            data_senders.insert(path, sender);
            routines.push(routine);
        }
        for hook in self.track_hooks {
            let path = hook.path.clone();
            let (sender, routine) = hook.start();
            track_senders.insert(path, sender);
            routines.push(routine);
        }
        loop {
            futures::select! {
                failed_hook_path = routines.next().fuse() => {
                    if let Some(failed_hook_path) = failed_hook_path {
                        data_senders.remove(&failed_hook_path);
                        track_senders.remove(&failed_hook_path);
                    } else {
                        tracing::error!("all hook routines have stopped");
                        break;
                    }
                }
                n = self.rx.recv().fuse() => {
                    match n {
                        Some(Notification::Data(d)) => for (path, sender) in &data_senders {
                            if let Err(_) = sender.try_send(d.clone()) {
                                tracing::warn!(hook=%path.display(), "dropping data message for hook which is running too slowly");
                            }
                        },
                        Some(Notification::Track(t)) => for (path, sender) in &track_senders {
                            if let Err(_) = sender.try_send(t.clone()) {
                                tracing::warn!(hook=%path.display(), "dropping track message for hook which is running too slowly");
                            }
                        },
                        None => break,
                    }
                },
                _ = stop => {
                    tracing::info!("hook routines shutting down");
                    break;
                }
            }
        }

        // Send EOTs to all senders

        // wait for subprocesses to finish with timeout

        // Kill any remaining
        //
        // Go home
    }

    //async fn dispatch(&mut self, data: Notification<R>) {
    //}
}

//pub async fn notify<P, R, S>(mut hooks: Hooks<P, R>, mut data: S)
//where
//P: Handle + Send + Sync + 'static,
//S: Stream<Item = Notification<R>> + Unpin,
//{
//while let Some(d) = data.next().await {
//for hook in hooks.iter_mut() {
//if let Err(err) = hook.write(d.display().as_bytes()).await {
//tracing::warn!(path = %hook.path.display(), err = %err, "failed to write to hook");
//}
//}
//}

//for hook in hooks.iter_mut() {
//if let Err(err) = hook.write(&[EOT]).await {
//tracing::warn!(path = %hook.path.display(), err = %err, "failed to write EOT to hook");
//}
//}
//}
