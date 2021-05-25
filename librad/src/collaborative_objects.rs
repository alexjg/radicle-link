// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use crate::git::{
    identities::{any::get as get_identity, local::LocalIdentity},
    refs::{self, Refs},
    storage::{read::Error as ReadError, ReadOnlyStorage, Storage},
    types::{Namespace, Reference, RefsCategory},
};

use std::{collections::HashMap, str::FromStr};

pub use cob::{
    ChangeGraphInfo,
    CollaborativeObject,
    History,
    NewObjectSpec,
    ObjectId,
    ObjectRefs,
    RefsStorage,
    TypeName,
    UpdateObjectSpec,
};
use either::Either;
use link_crypto::BoxedSigner;
use link_identities::git::{Person, Project, SomeIdentity, Urn};

mod error {
    use super::RefsError;
    use crate::git::identities::Error as IdentitiesError;
    use link_identities::git::Urn;
    use thiserror::Error;

    #[allow(clippy::large_enum_variant)]
    #[derive(Debug, Error)]
    pub enum Create {
        #[error(transparent)]
        Cob(#[from] cob::error::Create<RefsError>),
        #[error(transparent)]
        Identities(#[from] IdentitiesError),
        #[error("no identity found for {urn}")]
        NoSuchIdentity { urn: Urn },
        #[error(transparent)]
        UnknownIdentity(#[from] UnknownIdentityType),
    }

    #[allow(clippy::large_enum_variant)]
    #[derive(Debug, Error)]
    pub enum Retrieve {
        #[error(transparent)]
        Cob(#[from] cob::error::Retrieve<RefsError>),
        #[error(transparent)]
        Identities(#[from] IdentitiesError),
        #[error("no identity found for {urn}")]
        NoSuchIdentity { urn: Urn },
        #[error(transparent)]
        UnknownIdentity(#[from] UnknownIdentityType),
    }

    #[allow(clippy::large_enum_variant)]
    #[derive(Debug, Error)]
    pub enum Update {
        #[error(transparent)]
        Cob(#[from] cob::error::Update<RefsError>),
        #[error(transparent)]
        Identities(#[from] IdentitiesError),
        #[error("no identity found for {urn}")]
        NoSuchIdentity { urn: Urn },
        #[error(transparent)]
        UnknownIdentity(#[from] UnknownIdentityType),
    }

    #[allow(clippy::large_enum_variant)]
    #[derive(Debug, Error)]
    #[error("unknown identity type")]
    pub struct UnknownIdentityType {}
}

pub struct CollaborativeObjects<'a> {
    signer: BoxedSigner,
    store: &'a Storage,
    repo: &'a git2::Repository,
    cache_dir: Option<std::path::PathBuf>,
}

impl<'a> CollaborativeObjects<'a> {
    pub fn new(
        signer: BoxedSigner,
        store: &'a Storage,
        repo: &'a git2::Repository,
        cache_dir: Option<std::path::PathBuf>,
    ) -> CollaborativeObjects<'a> {
        CollaborativeObjects {
            signer,
            store,
            repo,
            cache_dir,
        }
    }

    pub fn create_object(
        &self,
        whoami: &LocalIdentity,
        within_identity: &Urn,
        spec: cob::NewObjectSpec,
    ) -> Result<cob::CollaborativeObject, error::Create> {
        let identity = get_identity(self.store, within_identity)?.ok_or_else(|| {
            error::Create::NoSuchIdentity {
                urn: within_identity.clone(),
            }
        })?;
        cob::create_object(
            self,
            self.repo,
            &self.signer,
            whoami,
            refine_identity(identity)?,
            spec,
            self.cache_dir.clone(),
        )
        .map_err(error::Create::from)
    }

    pub fn retrieve_object(
        &self,
        identity_urn: &Urn,
        typename: &cob::TypeName,
        oid: &cob::ObjectId,
    ) -> Result<Option<cob::CollaborativeObject>, error::Retrieve> {
        let identity = get_identity(self.store, identity_urn)?.ok_or_else(|| {
            error::Retrieve::NoSuchIdentity {
                urn: identity_urn.clone(),
            }
        })?;
        cob::retrieve_object(
            self,
            self.repo,
            refine_identity(identity)?,
            typename,
            oid,
            self.cache_dir.clone(),
        )
        .map_err(error::Retrieve::from)
    }

    pub fn retrieve_objects(
        &self,
        identity_urn: &Urn,
        typename: &cob::TypeName,
    ) -> Result<Vec<cob::CollaborativeObject>, error::Retrieve> {
        let identity = get_identity(self.store, identity_urn)?.ok_or_else(|| {
            error::Retrieve::NoSuchIdentity {
                urn: identity_urn.clone(),
            }
        })?;
        cob::retrieve_objects(
            self,
            self.repo,
            refine_identity(identity)?,
            typename,
            self.cache_dir.clone(),
        )
        .map_err(error::Retrieve::from)
    }

    pub fn update_object(
        &self,
        whoami: &LocalIdentity,
        within_identity: &Urn,
        spec: UpdateObjectSpec,
    ) -> Result<cob::CollaborativeObject, error::Update> {
        let identity = get_identity(self.store, within_identity)?.ok_or_else(|| {
            error::Update::NoSuchIdentity {
                urn: within_identity.clone(),
            }
        })?;
        cob::update_object(
            self,
            &self.signer,
            self.repo,
            whoami,
            refine_identity(identity)?,
            spec,
            self.cache_dir.clone(),
        )
        .map_err(error::Update::from)
    }

    pub fn changegraph_info_for_object(
        &self,
        identity_urn: &Urn,
        typename: &cob::TypeName,
        oid: &cob::ObjectId,
    ) -> Result<Option<ChangeGraphInfo>, error::Retrieve> {
        let identity = get_identity(self.store, identity_urn)?.ok_or_else(|| {
            error::Retrieve::NoSuchIdentity {
                urn: identity_urn.clone(),
            }
        })?;
        cob::changegraph_info_for_object(self, self.repo, refine_identity(identity)?, typename, oid)
            .map_err(error::Retrieve::from)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RefsError {
    #[error(transparent)]
    Git2(#[from] git2::Error),
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Refs(#[from] refs::stored::Error),
}

impl<'a> RefsStorage for CollaborativeObjects<'a> {
    type Error = RefsError;

    fn object_references<'b>(
        &'b self,
        project_urn: &identities::git::Urn,
        typename: &TypeName,
        oid: &ObjectId,
    ) -> Result<cob::ObjectRefs<'b>, Self::Error> {
        let mut local = None;
        if let Some(local_ref) = local_ref(self.store, project_urn, typename, oid)? {
            local = Some(local_ref);
        }
        let glob = remote_glob(project_urn, typename, oid);
        let mut remote = Vec::new();
        let remote_refs: Vec<git2::Reference> = self
            .store
            .references_glob(glob.compile_matcher())?
            .flatten()
            .collect();
        remote.extend(remote_refs);
        Ok(cob::ObjectRefs { local, remote })
    }

    fn type_references<'b>(
        &'b self,
        project_urn: &Urn,
        typename: &TypeName,
    ) -> Result<HashMap<ObjectId, ObjectRefs<'b>>, Self::Error> {
        let matcher = ObjRefMatcher::new(project_urn, typename);

        let refs: git2::References<'a> = self.repo.references()?;
        let mut result = HashMap::new();
        for reference in refs.into_iter() {
            let reference = reference?;
            if let Some(name) = reference.name() {
                match matcher.match_ref(name) {
                    ObjRefMatch::Local(oid) => {
                        result.entry(oid).or_insert_with(|| ObjectRefs {
                            local: Some(reference),
                            remote: Vec::new(),
                        });
                    },
                    ObjRefMatch::Remote(oid) => {
                        let refs = result.entry(oid).or_insert_with(|| ObjectRefs {
                            local: None,
                            remote: Vec::new(),
                        });
                        refs.remote.push(reference);
                    },
                    ObjRefMatch::NoMatch => {},
                }
            }
        }
        Ok(result)
    }

    fn update_ref(
        &self,
        project_urn: &Urn,
        typename: &TypeName,
        object_id: ObjectId,
        new_commit: git2::Oid,
    ) -> Result<(), Self::Error> {
        let reference = Reference::rad_collaborative_object(
            Namespace::from(project_urn.clone()),
            None,
            typename.clone(),
            object_id,
        );

        tracing::info!(reference=%reference, commit=?new_commit, "adding change to collaborative object");
        self.repo
            .reference(&reference.to_string(), new_commit, true, "new change")?;

        Refs::update(self.store, project_urn)?;
        Ok(())
    }
}

fn local_ref<'a, S: ReadOnlyStorage>(
    store: &'a S,
    project_urn: &Urn,
    typename: &TypeName,
    oid: &ObjectId,
) -> Result<Option<git2::Reference<'a>>, RefsError> {
    let reference = Reference::rad_collaborative_object(
        Namespace::from(project_urn.clone()),
        None,
        typename.clone(),
        *oid,
    );

    store.reference(&reference).map_err(|e| e.into())
}

fn remote_glob(identity_urn: &Urn, typename: &TypeName, oid: &ObjectId) -> globset::Glob {
    let namespace = Namespace::from(identity_urn);

    globset::Glob::new(
        format!(
            "refs/namespaces/{}/refs/remotes/**/{}/{}/{}",
            namespace.to_string(),
            RefsCategory::Cob.to_string(),
            typename.to_string(),
            oid.to_string(),
        )
        .as_str(),
    )
    .unwrap()
}

fn refine_identity(
    id: SomeIdentity,
) -> Result<Either<Person, Project>, error::UnknownIdentityType> {
    match id {
        SomeIdentity::Person(p) => Ok(Either::Left(p)),
        SomeIdentity::Project(p) => Ok(Either::Right(p)),
        _ => Err(error::UnknownIdentityType {}),
    }
}

#[derive(Debug, PartialEq)]
pub enum ObjRefMatch {
    Local(ObjectId),
    Remote(ObjectId),
    NoMatch,
}

pub struct ObjRefMatcher {
    remote_regex: regex::Regex,
    local_regex: regex::Regex,
}

impl ObjRefMatcher {
    pub fn new(project_urn: &Urn, typename: &TypeName) -> ObjRefMatcher {
        let remote_ref_str = format!(
            r"refs/namespaces/{}/refs/remotes/([0-9a-zA-Z]+)/{}/{}/([0-9a-f]{{40}})",
            project_urn.encode_id(),
            RefsCategory::Cob.to_string(),
            typename.regex_safe_string(),
        );
        let remote_regex = regex::Regex::new(remote_ref_str.as_str()).unwrap();

        let local_ref_str = format!(
            r"refs/namespaces/{}/refs/{}/{}/([0-9a-f]{{40}})",
            project_urn.encode_id(),
            RefsCategory::Cob.to_string(),
            typename.regex_safe_string(),
        );
        let local_regex = regex::Regex::new(local_ref_str.as_str()).unwrap();
        ObjRefMatcher {
            remote_regex,
            local_regex,
        }
    }

    pub fn match_ref(&self, ref_str: &str) -> ObjRefMatch {
        if let Some(cap) = self.remote_regex.captures(ref_str) {
            let oid_str = &cap[2];
            // This unwrap is safe due to the regular expression ensuring the string is a
            // valid object id
            let oid = ObjectId::from_str(oid_str).unwrap();
            ObjRefMatch::Remote(oid)
        } else if let Some(cap) = self.local_regex.captures(ref_str) {
            let oid_str = &cap[1];
            // Safe for the same reasoning as above
            let oid = ObjectId::from_str(oid_str).unwrap();
            ObjRefMatch::Local(oid)
        } else {
            ObjRefMatch::NoMatch
        }
    }
}
