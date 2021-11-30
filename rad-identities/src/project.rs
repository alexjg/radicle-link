// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{
    collections::BTreeSet,
    convert::TryFrom as _,
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
};

use either::Either;
use thiserror::Error;

use librad::{
    crypto::BoxedSigner,
    git::{
        identities::{self, local::LocalIdentity, project, relations, Project},
        local::{transport, url::LocalUrl},
        storage::{ReadOnly, Storage},
        types::{Namespace, Pushspec, Reference},
        Urn,
    },
    identities::{
        delegation::{indirect, Indirect},
        git::Revision,
        payload::{self, KeyOrUrn, ProjectPayload},
        IndirectDelegation,
    },
    paths::Paths,
    PeerId,
};

use crate::{
    display,
    git::{self, checkout, include, push},
    MissingDefaultIdentity,
};

pub type Display = display::Display<ProjectPayload>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Checkout(#[from] checkout::Error),

    #[error(transparent)]
    Ext(#[from] payload::ExtError),

    #[error(transparent)]
    Identities(Box<identities::Error>),

    #[error(transparent)]
    Include(#[from] Box<include::Error>),

    #[error(transparent)]
    Indirect(#[from] indirect::error::FromIter<Revision>),

    #[error(transparent)]
    Local(#[from] identities::local::Error),

    #[error(transparent)]
    MissingDefault(#[from] MissingDefaultIdentity),

    #[error(transparent)]
    Relations(Box<relations::Error>),

    #[error(transparent)]
    Push(#[from] push::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<relations::Error> for Error {
    fn from(err: relations::Error) -> Self {
        Self::Relations(Box::new(err))
    }
}

impl From<identities::Error> for Error {
    fn from(err: identities::Error) -> Self {
        Self::Identities(Box::new(err))
    }
}

impl From<include::Error> for Error {
    fn from(err: include::Error) -> Self {
        Self::Include(Box::new(err))
    }
}

pub enum Creation {
    New { path: Option<PathBuf> },
    Existing { path: PathBuf },
}

pub enum WhoAmI {
    Default,
    Urn(Urn),
}

impl From<Option<Urn>> for WhoAmI {
    fn from(urn: Option<Urn>) -> Self {
        urn.map_or(Self::Default, Self::Urn)
    }
}

impl WhoAmI {
    fn resolve(self, storage: &Storage) -> Result<LocalIdentity, Error> {
        Ok(match self {
            Self::Default => identities::local::default(storage)?.ok_or(MissingDefaultIdentity)?,
            Self::Urn(urn) => identities::local::load(storage, urn.clone())?
                .ok_or(identities::Error::NotFound(urn))?,
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub fn create<T>(
    storage: &Storage,
    paths: Paths,
    signer: BoxedSigner,
    whoami: WhoAmI,
    mut delegations: BTreeSet<KeyOrUrn<Revision>>,
    payload: payload::Project,
    ext: Vec<payload::Ext<T>>,
    creation: Creation,
) -> anyhow::Result<Project>
where
    T: serde::Serialize,
{
    let mut payload = ProjectPayload::new(payload);
    for e in ext.into_iter() {
        payload.set_ext(e)?;
    }

    let whoami = whoami.resolve(storage)?;
    delegations.insert(KeyOrUrn::from(Either::Right(whoami.urn())));
    let delegations = resolve_indirect(storage, delegations)?;

    let urn = project::urn(storage, payload.clone(), delegations.clone())?;
    let url = LocalUrl::from(urn);
    let settings = transport::Settings {
        paths: paths.clone(),
        signer,
    };

    let project = match creation {
        Creation::New { path } => {
            if let Some(path) = path {
                let valid = git::new::New::new(payload.clone(), path).validate()?;
                let project = project::create(storage, whoami, payload, delegations)?;
                valid.init(url, settings)?;
                project
            } else {
                project::create(storage, whoami, payload, delegations)?
            }
        },
        Creation::Existing { path } => {
            let valid = git::existing::Existing::new(payload.clone(), path).validate()?;
            let project = project::create(storage, whoami, payload, delegations)?;
            valid.init(url, settings)?;
            project
        },
    };

    include::update(storage, &paths, &project)?;

    Ok(project)
}

pub fn get<S>(storage: &S, urn: &Urn) -> Result<Option<Project>, Error>
where
    S: AsRef<ReadOnly>,
{
    Ok(project::get(storage, urn)?)
}

pub fn list<S>(
    storage: &S,
) -> Result<impl Iterator<Item = Result<Project, identities::Error>> + '_, Error>
where
    S: AsRef<ReadOnly>,
{
    Ok(crate::any::list(storage, |i| i.project())?)
}

pub fn update(
    storage: &Storage,
    urn: &Urn,
    whoami: Option<Urn>,
    payload: Option<payload::Project>,
    mut ext: Vec<payload::Ext<serde_json::Value>>,
    delegations: BTreeSet<KeyOrUrn<Revision>>,
) -> Result<Project, Error> {
    let old =
        project::verify(storage, urn)?.ok_or_else(|| identities::Error::NotFound(urn.clone()))?;
    let mut old_payload = old.payload().clone();
    let payload = match payload {
        None => {
            for e in ext {
                old_payload.set_ext(e)?;
            }
            old_payload
        },
        Some(payload) => {
            let mut payload = payload::ProjectPayload::new(payload);
            ext.extend(old_payload.exts().map(|(url, val)| payload::Ext {
                namespace: url.clone(),
                val: val.clone(),
            }));
            for e in ext {
                payload.set_ext(e)?;
            }
            payload
        },
    };
    let whoami = whoami
        .and_then(|me| identities::local::load(storage, me).transpose())
        .transpose()?;

    let delegations = if delegations.is_empty() {
        None
    } else {
        Some(resolve_indirect(storage, delegations)?)
    };
    Ok(project::update(storage, urn, whoami, payload, delegations)?)
}

fn resolve_indirect(
    storage: &Storage,
    delegations: BTreeSet<KeyOrUrn<Revision>>,
) -> Result<IndirectDelegation, Error> {
    Ok(Indirect::try_from_iter(
        delegations
            .into_iter()
            .map(|kou| match kou.into() {
                Either::Left(key) => Ok(Either::Left(key)),
                Either::Right(urn) => identities::person::verify(storage, &urn)
                    .and_then(|person| person.ok_or(identities::Error::NotFound(urn)))
                    .map(|person| Either::Right(person.into_inner())),
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter(),
    )?)
}

pub fn checkout<S>(
    storage: &S,
    paths: Paths,
    signer: BoxedSigner,
    urn: &Urn,
    peer: Option<PeerId>,
    path: PathBuf,
) -> Result<git2::Repository, Error>
where
    S: AsRef<ReadOnly>,
{
    let local = storage.as_ref().peer_id();
    let project = get(storage, urn)?.ok_or_else(|| identities::Error::NotFound(urn.clone()))?;
    let remote = peer
        .and_then(|peer| {
            if peer == *local {
                None
            } else {
                let urn = Urn::try_from(Reference::rad_self(Namespace::from(&project.urn()), peer))
                    .expect("namespace is set");
                Some(identities::person::get(&storage, &urn).and_then(|person| {
                    person
                        .ok_or_else(|| identities::Error::NotFound(urn.clone()))
                        .map(|person| (person, peer))
                }))
            }
        })
        .transpose()?;
    let from = checkout::from_whom(&project, remote, path)?;
    let settings = transport::Settings {
        paths: paths.clone(),
        signer,
    };
    let repo = git::checkout::checkout(settings, &project, from)?;
    include::update(&storage, &paths, &project)?;
    Ok(repo)
}

pub fn tracked<S>(storage: &S, urn: &Urn) -> Result<relations::Tracked, Error>
where
    S: AsRef<ReadOnly>,
{
    // ensure that the URN exists and is indeed a project
    let _guard = get(storage, urn)?.ok_or_else(|| identities::Error::NotFound(urn.clone()))?;
    Ok(identities::relations::tracked(storage, urn)?)
}

pub fn push<P>(
    paths: &Paths,
    signer: BoxedSigner,
    repo_path: P,
    spec: Pushspec,
) -> Result<(), Error>
where
    P: AsRef<Path>,
{
    let settings = transport::Settings {
        paths: paths.clone(),
        signer,
    };
    let (urn, updated_refs) = git::push::push(settings, repo_path, spec)?;
    let messages = updated_refs.map(|updated_branch| {
        let git::push::UpdatedBranch { branch, oid } = updated_branch;
        node_lib::api::wire_types::events::Envelope {
            message: Some(node_lib::api::wire_types::events::Message::PostReceive(
                node_lib::api::wire_types::events::PostReceive {
                    urn: urn.clone().with_path(branch),
                    rev: oid.into(),
                },
            )),
        }
    });
    let event_socket = paths.events_socket();
    let stream = UnixStream::connect(event_socket)?;
    for message in messages {
        minicbor::encode(message, &stream).map_err(|e| {
            let msg = format!("error writing message to p2p socket: {}", e);
            Error::Io(std::io::Error::new(std::io::ErrorKind::Other, msg))
        })?;
    }
    Ok(())
}
