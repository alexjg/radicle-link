// Copyright © 2022 The Radicle Link Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod data;
use std::fmt;

pub use data::Data;

pub mod track;
use link_identities::urn::HasProtocol;
use multihash::Multihash;
pub use track::Track;

pub enum Event {
    Track,
    Data,
}

pub trait IsZero {
    fn is_zero(&self) -> bool;
}

/// End of transimission character.
pub const EOT: i32 = 0x04;

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

pub trait Handle {
    type WriteError: std::error::Error + Send + Sync + 'static;

    fn write(&mut self, bs: &[u8]) -> Result<(), Self::WriteError>;
}

pub trait Hook {
    type Handle: Handle;
    type HandleError: std::error::Error + Send + Sync + 'static;

    fn handle(&self) -> Result<Self::Handle, Self::HandleError>;
}

pub struct NotifyData<R> {
    data: Vec<Data<R>>,
}

// TODO(finto): We may want to be able to interleave notifying new updates until
// the EOT is sent. For example, track peer A, send notification, do more
// things, track peer B, send notification, send EOT.

// TODO(finto): handle and write would probably need to be async to allow for
// interleaving.

// TODO(finto): can unify with a trait that "renders" the hook input, but this
// trait should be sealed for Data and Track.
impl<R> NotifyData<R>
where
    R: HasProtocol + fmt::Display,
    for<'a> &'a R: Into<Multihash>,
{
    pub fn notify<H: Hook>(
        &self,
        hook: H,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut handle = hook.handle().map_err(Box::new)?;
        for data in &self.data {
            handle
                .write(data.to_string().as_bytes())
                .map_err(Box::new)?;
        }

        Ok(())
    }
}

pub struct NotifyTrack<R> {
    track: Vec<Track<R>>,
}

impl<R> NotifyTrack<R>
where
    R: HasProtocol + fmt::Display,
    for<'a> &'a R: Into<Multihash>,
{
    pub fn notify<H: Hook>(
        &self,
        hook: H,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut handle = hook.handle().map_err(Box::new)?;
        for track in &self.track {
            handle
                .write(track.to_string().as_bytes())
                .map_err(Box::new)?;
        }

        Ok(())
    }
}
