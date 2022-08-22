use std::str::FromStr;

use librad::{PeerId, git::Urn, git_ext};

/// A wrapper around Urn which parses strings of the form "rad:git:<id>.git",
/// this is used as the path parameter of `link_git::SshService`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrnPath{
    urn: Urn,
    peer: Option<PeerId>,
}

impl UrnPath {
    pub fn new(urn: Urn, peer: Option<PeerId>) -> Self {
        Self {
            urn,
            peer,
        }
    }
}

pub type SshService = link_git::service::SshService<UrnPath>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("path should be rad:git:<urn>[/<peer ID>]")]
    BadFormat,
    #[error(transparent)]
    Urn(#[from] librad::identities::urn::error::FromStr<git_ext::oid::FromMultihashError>),
    #[error(transparent)]
    PeerId(#[from] librad::crypto::peer::conversion::Error),
}

impl std::fmt::Display for UrnPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.git", self.urn)?;
        if let Some(peer_id) = self.peer {
            write!(f, "/{}", peer_id)?;
        }
        Ok(())
    }
}

impl AsRef<Urn> for UrnPath {
    fn as_ref(&self) -> &Urn {
        &self.urn
    }
}

lazy_static::lazy_static!{
    static ref URN_PATH: regex::Regex = regex::Regex::new(r"(rad:git:([a-zA-Z0-9]+))(/(?P<peer>[a-zA-Z0-9]+))?.git").unwrap();
}

impl FromStr for UrnPath {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(caps) = URN_PATH.captures(s) {
            // SAFETY The regex always captures one group if it succeeds
            let urn_str = caps.get(1).unwrap();
            let urn = Urn::from_str(urn_str.as_str())?;
            let peer_id = if let Some(peer_cap) = caps.name("peer") {
                Some(PeerId::from_str(peer_cap.as_str())?)
            } else {
                None
            };
            Ok(Self::new(urn, peer_id))
        } else {
            Err(Error::BadFormat)
        }
    }
}

impl From<UrnPath> for Urn {
    fn from(u: UrnPath) -> Self {
        u.urn
    }
}
