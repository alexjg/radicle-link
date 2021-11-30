// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use librad::{
    data::NonEmptyVec,
    git::{
        local::{transport::Error as TransportError, url::LocalUrl},
        types::{
            remote::{FindError, LocalPushspec},
            Pushspec,
        },
        Urn,
    },
};
use radicle_git_ext::RefLike;
use std::path::Path;

use librad::{
    git::{local::transport::CanOpenStorage, types::Remote},
    reflike,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no rad remote")]
    NoRadRemote,

    #[error(transparent)]
    Git(#[from] git2::Error),

    #[error(transparent)]
    FindRemote(#[from] FindError),

    #[error(transparent)]
    Transport(#[from] TransportError),
}

pub struct UpdatedBranch {
    pub branch: RefLike,
    pub oid: git2::Oid,
}

/// Given a repository with a `rad` remote, push the contents of the repository
/// to the local radicle monorepo, update the signed refs and return the updated
/// refs
pub fn push<C, P>(
    storage: C,
    local_repo_path: P,
    spec: Pushspec,
) -> Result<(Urn, impl Iterator<Item = UpdatedBranch>), Error>
where
    P: AsRef<Path>,
    C: CanOpenStorage + 'static,
{
    let repo = git2::Repository::open(local_repo_path)?;
    let mut remote: Remote<LocalUrl> =
        Remote::find(&repo, reflike!("rad"))?.ok_or(Error::NoRadRemote)?;
    let local_spec = LocalPushspec::Specs(NonEmptyVec::new(spec));
    let updated_refs = remote
        .push(storage, &repo, local_spec)?
        .filter_map(move |reference| {
            repo.find_reference(reference.to_string().as_str())
                .ok()
                .and_then(|r| r.target())
                .map(|oid| UpdatedBranch {
                    branch: reference,
                    oid,
                })
        });
    Ok((remote.url.urn, updated_refs))
}
