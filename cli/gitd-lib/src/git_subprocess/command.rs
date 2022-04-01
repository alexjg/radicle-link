// Copyright © 2021 The Radicle Link Contributors
// Copyright © 2019-2020 The Radicle Foundation <hello@radicle.foundation>
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::fmt::Debug;

use git2::transport::Service as GitService;

use librad::{
    git::{
        storage::{self, glob, ReadOnlyStorage as _},
        types::Namespace,
        Urn,
    },
    reflike,
};
use link_git::service::SshService;
use radicle_git_ext as ext;

#[derive(thiserror::Error, Debug)]
pub(super) enum Error {
    #[error("no such URN {0}")]
    NoSuchUrn(Urn),
    #[error("error fetching references glob {glob} for {urn}: {error}")]
    FetchRefsGlob {
        urn: Urn,
        error: Box<dyn std::error::Error + Send + 'static>,
        glob: String,
    },
    #[error("error iterating refs for {urn}: {error}")]
    IterateRefs {
        urn: Urn,
        error: Box<dyn std::error::Error + Send + 'static>,
    },
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + 'static>),
}

// This is largely copied from librad::git::local::transport. Whilst it is
// basically the same logic it doesn't seem ideal to expose this logic as a part
// of librads public API and it doesn't seem like enough code to warrant a new
// crate.
pub(super) fn create_command(
    storage: &storage::Storage,
    service: SshService<Urn>,
) -> Result<tokio::process::Command, Error> {
    guard_has_urn(storage, &service.path)?;

    let mut git = tokio::process::Command::new("git");
    git.current_dir(&storage.path()).args(&[
        &format!("--namespace={}", Namespace::from(&service.path)),
        "-c",
        "transfer.hiderefs=refs/remotes",
        "-c",
        "transfer.hiderefs=refs/remotes/rad",
        "-c",
        "transfer.hiderefs=refs/remotes/cobs",
    ]);

    match service.service.0 {
        GitService::UploadPack | GitService::UploadPackLs => {
            // Fetching remotes is ok, pushing is not
            visible_remotes(storage, &service.path)?.for_each(|remote_ref| {
                git.arg("-c")
                    .arg(format!("uploadpack.hiderefs=!^{}", remote_ref));
            });
            git.args(&["upload-pack", "--strict", "--timeout=5"]);
        },

        GitService::ReceivePack | GitService::ReceivePackLs => {
            git.arg("receive-pack");
        },
    }

    if matches!(
        service.service.0,
        GitService::UploadPackLs | GitService::ReceivePackLs
    ) {
        git.arg("--advertise-refs");
    }
    Ok(git)
}

fn guard_has_urn<S>(storage: S, urn: &Urn) -> Result<(), Error>
where
    S: AsRef<librad::git::storage::ReadOnly>,
{
    let have = storage
        .as_ref()
        .has_urn(urn)
        .map_err(|e| Error::Other(Box::new(e)))?;
    if !have {
        Err(Error::NoSuchUrn(urn.clone()))
    } else {
        Ok(())
    }
}

fn visible_remotes<S>(storage: S, urn: &Urn) -> Result<impl Iterator<Item = ext::RefLike>, Error>
where
    S: AsRef<librad::git::storage::ReadOnly>,
{
    let glob = visible_remotes_glob(urn);
    let remotes = storage
        .as_ref()
        .references_glob(visible_remotes_glob(urn))
        .map_err(|e| {
            tracing::error!(err=?e, ?urn, ?glob, "error fetching references glob for urn");
            Error::FetchRefsGlob {
                error: Box::new(e),
                urn: urn.clone(),
                glob: format!("{:?}", glob),
            }
        })?
        .filter_map(move |res| {
            res.map(|reference| {
                reference
                    .name()
                    .and_then(|name| ext::RefLike::try_from(name).ok())
            })
            .map_err(|e| {
                tracing::error!(err=?e, "error resolving reference names");
                Error::IterateRefs {
                    error: Box::new(e),
                    urn: urn.clone(),
                }
            })
            .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(remotes.into_iter())
}

pub fn visible_remotes_glob(urn: &Urn) -> impl glob::Pattern + Debug {
    globset::Glob::new(&format!(
        "{}/*/{{[!cobs],[!rad]}}/*",
        reflike!("refs/namespaces")
            .join(Namespace::from(urn))
            .join(reflike!("refs/remotes"))
    ))
    .unwrap()
    .compile_matcher()
}
