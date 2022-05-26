// Copyright © 2022 The Radicle Link Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#[macro_use]
extern crate async_trait;

use std::path::PathBuf;

use futures::{Stream, StreamExt as _};

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

pub struct Hook<P: Handle> {
    path: PathBuf,
    child: P,
}

impl<P: Handle> Hook<P> {
    pub fn new(path: PathBuf, child: P) -> Self {
        Self { path, child }
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

pub async fn notify<P, N, S>(mut hooks: Vec<Hook<P>>, mut data: S)
where
    P: Handle + Send + Sync + 'static,
    S: Stream<Item = N> + Unpin,
    N: Display,
{
    while let Some(d) = data.next().await {
        for hook in hooks.iter_mut() {
            if let Err(err) = hook.write(d.display().as_bytes()).await {
                tracing::warn!(path = %hook.path.display(), err = %err, "failed to write to hook");
            }
        }
    }

    for hook in hooks.iter_mut() {
        if let Err(err) = hook.write(&[EOT]).await {
            tracing::warn!(path = %hook.path.display(), err = %err, "failed to write EOT to hook");
        }
    }
}
