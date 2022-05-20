// Copyright © 2022 The Radicle Link Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use std::fmt;

use link_crypto::PeerId;
use link_identities::urn::{HasProtocol, Urn};
use multihash::Multihash;

pub enum Event {
    Track,
    Data,
}

pub trait IsZero {
    fn is_zero(&self) -> bool;
}

pub struct Data<R> {
    urn: Urn<R>,
    old: R,
    new: R,
}

impl<R> fmt::Display for Data<R>
where
    R: HasProtocol + fmt::Display,
    for<'a> &'a R: Into<Multihash>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rad:{} {} ", R::PROTOCOL, self.urn.encode_id())?;

        if let Some(path) = &self.urn.path {
            write!(f, "{} ", path)?;
        }

        write!(f, "{} {}\n", self.old, self.new)
    }
}

impl<R> Data<R>
where
    R: IsZero + PartialEq,
{
    pub fn is_deleted(&self) -> bool {
        self.new.is_zero() && !self.old.is_zero()
    }

    pub fn is_created(&self) -> bool {
        !self.new.is_zero() && self.old.is_zero()
    }

    pub fn is_changed(&self) -> bool {
        self.new != self.old
    }
}

pub struct Track<R> {
    urn: Urn<R>,
    peer: Option<PeerId>,
    old: R,
    new: R,
}

impl<R> Track<R>
where
    R: IsZero + PartialEq,
{
    pub fn is_deleted(&self) -> bool {
        self.new.is_zero() && !self.old.is_zero()
    }

    pub fn is_created(&self) -> bool {
        !self.new.is_zero() && self.old.is_zero()
    }

    pub fn is_changed(&self) -> bool {
        self.new != self.old
    }
}

impl<R> fmt::Display for Track<R>
where
    R: HasProtocol + fmt::Display,
    for<'a> &'a R: Into<Multihash>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rad:{} {} ", R::PROTOCOL, self.urn.encode_id())?;

        match self.peer {
            None => write!(f, "default "),
            Some(peer) => write!(f, "{} ", peer),
        }?;

        write!(f, "{} {}\n", self.old, self.new)
    }
}
