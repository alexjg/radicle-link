// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{collections::HashSet, convert::TryFrom, fmt, str::FromStr};

use either::Either;
use serde::{Deserialize, Serialize};

use link_crypto::BoxedSigner;
use link_identities::git::{Person, Project, Urn};
use radicle_git_ext as ext;

mod change_metadata;
mod trailers;

mod change_graph;
use change_graph::ChangeGraph;

mod schema;
use schema::Schema;

mod change;
use change::Change;

mod identity_cache;
use identity_cache::IdentityCache;

mod schema_change;
use schema_change::SchemaChange;

mod refs_storage;
pub use refs_storage::{ObjectRefs, RefsStorage};

mod cache;
use cache::ThinChangeGraph;

mod validated_automerge;
use validated_automerge::ValidatedAutomerge;

/// The CRDT history for a collaborative object. Currently the only
/// implementation uses automerge
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum History {
    Automerge(Vec<u8>),
}

impl History {
    fn as_bytes(&self) -> &[u8] {
        match self {
            History::Automerge(h) => h,
        }
    }
}

impl AsRef<[u8]> for History {
    fn as_ref(&self) -> &[u8] {
        match self {
            History::Automerge(b) => b,
        }
    }
}

/// The data required to create a new object
pub struct NewObjectSpec {
    /// A valid JSON schema which uses the vocabulary at https://alexjg.github.io/automerge-jsonschema/spec
    pub schema_json: serde_json::Value,
    /// The CRDT history to initialize this object with
    pub history: History,
    /// The typename for this object
    pub typename: TypeName,
    /// An optional message to add to the commit message for the commit which
    /// creates this object
    pub message: Option<String>,
}

impl NewObjectSpec {
    fn typename(&self) -> TypeName {
        self.typename.clone()
    }

    fn change_spec(&self, schema_commit: git2::Oid) -> change::NewChangeSpec {
        change::NewChangeSpec {
            schema_commit,
            typename: self.typename.clone(),
            tips: None,
            message: self.message.clone(),
            history: self.history.clone(),
        }
    }
}

/// The data required to update a collaborative object
pub struct UpdateObjectSpec {
    /// The object ID of the object to be updated
    pub object_id: ObjectId,
    /// The typename of the object to be updated
    pub typename: TypeName,
    /// An optional message to add to the commit message of the change
    pub message: Option<String>,
    /// The CRDT changes to add to the object
    pub changes: History,
}

/// The typename of an object. Valid typenames MUST be sequences of alphanumeric
/// characters separated by a period. The name must start and end with an
/// alphanumeric character
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TypeName(String);

impl TypeName {
    /// A string representation of the typename which will match the typename in
    /// regular expressions. This primarily escapes periods
    pub fn regex_safe_string(&self) -> String {
        self.0.replace(".", "\\.")
    }
}

impl fmt::Display for TypeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0.as_str())
    }
}

impl FromStr for TypeName {
    type Err = error::TypeNameParse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let regex = regex::Regex::new(r"^([a-zA-Z0-9])+(\.[a-zA-Z0-9]+)*$").unwrap();
        if regex.is_match(s) {
            Ok(TypeName(s.to_string()))
        } else {
            Err(error::TypeNameParse {})
        }
    }
}

/// The id of an object
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ObjectId(git2::Oid);

impl FromStr for ObjectId {
    type Err = error::ParseObjectId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        git2::Oid::from_str(s)
            .map(ObjectId)
            .map_err(error::ParseObjectId::from)
    }
}

impl From<git2::Oid> for ObjectId {
    fn from(oid: git2::Oid) -> Self {
        ObjectId(oid)
    }
}

impl From<ext::Oid> for ObjectId {
    fn from(oid: ext::Oid) -> Self {
        git2::Oid::from(oid).into()
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.0.to_string().as_str())
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        ObjectId::from_str(&raw).map_err(serde::de::Error::custom)
    }
}

impl From<&git2::Oid> for ObjectId {
    fn from(oid: &git2::Oid) -> Self {
        ObjectId(*oid)
    }
}

/// A collaborative object
#[derive(Debug, Clone)]
pub struct CollaborativeObject {
    /// The identity (person or project) this collaborative object lives within
    containing_identity: Either<Person, Project>,
    /// The typename of this object
    typename: TypeName,
    /// The CRDT history we know about for this object
    history: History,
    /// The id of the object
    id: ObjectId,
    /// The schema any changes to this object must respect
    schema: Schema,
}

impl CollaborativeObject {
    pub fn history(&self) -> &History {
        &self.history
    }

    pub fn id(&self) -> &ObjectId {
        &self.id
    }

    pub fn typename(&self) -> &TypeName {
        &self.typename
    }
}

/// Additional information about the change graph of an object
pub struct ChangeGraphInfo {
    /// The ID of the object
    pub object_id: ObjectId,
    /// A graphviz description of the changegraph of the object
    pub dotviz: String,
    /// The number of nodes in the change graph of the object
    pub number_of_nodes: u64,
    /// The "tips" of the change graph, i.e the object IDs pointed to by
    /// references to the object
    pub tips: HashSet<git2::Oid>,
}

pub mod error {
    use super::{
        cache::Error as CacheError,
        change,
        change_graph::Error as ChangeGraphError,
        schema,
        schema_change,
    };
    use thiserror::Error;

    #[derive(Error, Debug)]
    #[error("invalid typename")]
    pub struct TypeNameParse {}

    #[derive(Debug, Error)]
    pub enum Create<RefsError: std::error::Error> {
        #[error("Invalid automerge history")]
        InvalidAutomergeHistory,
        #[error(transparent)]
        CreateSchemaChange(#[from] schema_change::error::Create),
        #[error(transparent)]
        CreateChange(#[from] change::error::Create),
        #[error("invalid schema: {0}")]
        InvalidSchema(#[from] schema::error::Parse),
        #[error(transparent)]
        Refs(RefsError),
        #[error(transparent)]
        Propose(#[from] super::validated_automerge::error::ProposalError),
        #[error(transparent)]
        Cache(#[from] CacheError),
    }

    #[derive(Debug, Error)]
    pub enum Retrieve<RefsError: std::error::Error> {
        #[error(transparent)]
        ChangeGraph(#[from] ChangeGraphError),
        #[error(transparent)]
        Git(#[from] git2::Error),
        #[error(transparent)]
        Refs(RefsError),
        #[error(transparent)]
        Cache(#[from] CacheError),
    }

    #[derive(Debug, Error)]
    pub enum Update<RefsError: std::error::Error> {
        #[error(transparent)]
        ChangeGraph(#[from] ChangeGraphError),
        #[error("no object found")]
        NoSuchObject,
        #[error(transparent)]
        CreateChange(#[from] change::error::Create),
        #[error(transparent)]
        Refs(RefsError),
        #[error(transparent)]
        Cache(#[from] CacheError),
        #[error(transparent)]
        Git(#[from] git2::Error),
        #[error(transparent)]
        Propose(#[from] super::validated_automerge::error::ProposalError),
    }

    #[derive(Debug, Error)]
    pub enum ParseObjectId {
        #[error(transparent)]
        Git2(#[from] git2::Error),
    }
}

/// Create a collaborative object
///
/// The `within_identity` argument specifies the identity (person or project)
/// this collaborative object will be referenced under.
pub fn create_object<R: RefsStorage, P: AsRef<std::path::Path>>(
    refs_storage: &R,
    repo: &git2::Repository,
    signer: &BoxedSigner,
    author: &Person,
    within_identity: Either<Person, Project>,
    spec: NewObjectSpec,
    cache_dir: Option<P>,
) -> Result<CollaborativeObject, error::Create<R::Error>> {
    let schema = Schema::try_from(&spec.schema_json)?;
    let schema_change = schema_change::SchemaChange::create(
        author.content_id.into(),
        repo,
        signer,
        schema.clone(),
    )?;

    let mut valid_history = ValidatedAutomerge::new(schema.clone());
    valid_history.propose_change(spec.history.as_ref())?;

    let init_change = change::Change::create(
        identity_oid(&within_identity),
        author.content_id.into(),
        repo,
        signer,
        spec.change_spec(schema_change.commit()),
    )
    .map_err(error::Create::from)?;

    let object_id = init_change.commit().into();
    refs_storage
        .update_ref(
            &identity_urn(&within_identity),
            &spec.typename(),
            object_id,
            *(init_change.commit()),
        )
        .map_err(error::Create::Refs)?;
    let mut cache = open_cache(cache_dir);
    let thin_graph = ThinChangeGraph::new_from_single_change(
        *init_change.author_commit(),
        schema.clone(),
        *init_change.schema_commit(),
        valid_history,
    );
    cache.put(init_change.commit().into(), thin_graph)?;
    Ok(CollaborativeObject {
        containing_identity: within_identity,
        typename: spec.typename(),
        history: spec.history,
        schema,
        id: init_change.commit().into(),
    })
}

/// Retrieve a collaborative object which is stored in the `within_identity`
/// person or project identity
pub fn retrieve_object<R: RefsStorage, P: AsRef<std::path::Path>>(
    refs_storage: &R,
    repo: &git2::Repository,
    within_identity: Either<Person, Project>,
    typename: &TypeName,
    oid: &ObjectId,
    cache_dir: Option<P>,
) -> Result<Option<CollaborativeObject>, error::Retrieve<R::Error>> {
    let tip_refs = refs_storage
        .object_references(&identity_urn(&within_identity), typename, oid)
        .map_err(error::Retrieve::Refs)?;
    tracing::trace!(refs=?tip_refs, "retrieving object");
    let tip_oids = tip_refs
        .iter()
        .map(|r| r.peel_to_commit().map(|c| c.id()))
        .collect::<Result<HashSet<git2::Oid>, git2::Error>>()?;
    let mut cache = open_cache(cache_dir);
    if let Some(obj) = cache.load(*oid, &tip_oids)? {
        return Ok(Some(CollaborativeObject {
            containing_identity: within_identity,
            typename: typename.clone(),
            history: obj.borrow().history(),
            schema: obj.borrow().schema().clone(),
            id: *oid,
        }));
    }
    if let Some(graph) = ChangeGraph::load(tip_refs.iter(), repo, &within_identity, typename, oid)?
    {
        let mut identities = IdentityCache::new(repo);
        let (object, valid_history) = graph.evaluate(&mut identities);
        let cached = cache::ThinChangeGraph::new(
            tip_oids,
            graph.schema().clone(),
            graph.schema_commit(),
            valid_history,
        );
        cache.put(*oid, cached)?;
        Ok(Some(object))
    } else {
        Ok(None)
    }
}

/// Retrieve all objects of a given type which are stored within the
/// `within_identity` person or project
pub fn retrieve_objects<R: RefsStorage, P: AsRef<std::path::Path>>(
    refs_storage: &R,
    repo: &git2::Repository,
    within_identity: Either<Person, Project>,
    typename: &TypeName,
    cache_dir: Option<P>,
) -> Result<Vec<CollaborativeObject>, error::Retrieve<R::Error>> {
    let references = refs_storage
        .type_references(&identity_urn(&within_identity), typename)
        .map_err(error::Retrieve::Refs)?;
    tracing::trace!(num_objects=?references.len(), "loaded references");
    let mut result = Vec::new();
    let mut cache = open_cache(cache_dir);
    let mut identities = IdentityCache::new(repo);
    for (oid, tip_refs) in &references {
        tracing::trace!(object_id=?oid, "loading object");
        let tip_oids = tip_refs
            .iter()
            .map(|r| r.peel_to_commit().map(|c| c.id()))
            .collect::<Result<HashSet<git2::Oid>, git2::Error>>()?;
        match cache.load(*oid, &tip_oids)? {
            Some(obj) => {
                tracing::trace!(object_id=?oid, "object found in cache");
                result.push(CollaborativeObject {
                    typename: typename.clone(),
                    id: *oid,
                    schema: obj.borrow().schema().clone(),
                    history: obj.borrow().history(),
                    containing_identity: within_identity.clone(),
                });
            },
            None => {
                tracing::trace!(object_id=?oid, "object not found in cache");
                if let Some(graph) =
                    ChangeGraph::load(tip_refs.iter(), repo, &within_identity, typename, oid)?
                {
                    let (object, valid_history) = graph.evaluate(&mut identities);
                    let cached = cache::ThinChangeGraph::new(
                        tip_oids,
                        graph.schema().clone(),
                        graph.schema_commit(),
                        valid_history,
                    );
                    cache.put(object.id, cached)?;
                    result.push(object)
                }
            },
        }
    }
    Ok(result)
}

pub fn update_object<R: RefsStorage, P: AsRef<std::path::Path>>(
    refs_storage: &R,
    signer: &BoxedSigner,
    repo: &git2::Repository,
    author: &Person,
    within_identity: Either<Person, Project>,
    spec: UpdateObjectSpec,
    cache_dir: Option<P>,
) -> Result<CollaborativeObject, error::Update<R::Error>> {
    let existing_refs = refs_storage
        .object_references(
            &identity_urn(&within_identity),
            &spec.typename,
            &spec.object_id,
        )
        .map_err(error::Update::Refs)?;

    let tip_oids = existing_refs
        .iter()
        .map(|r| r.peel_to_commit().map(|c| c.id()))
        .collect::<Result<HashSet<git2::Oid>, git2::Error>>()?;

    let mut cache = open_cache(cache_dir);
    let cached = if let Some(cached) = cache.load(spec.object_id, &tip_oids)? {
        cached
    } else if let Some(graph) = ChangeGraph::load(
        existing_refs.iter(),
        repo,
        &within_identity,
        &spec.typename,
        &spec.object_id,
    )? {
        let mut identities = IdentityCache::new(repo);
        let (_, valid_history) = graph.evaluate(&mut identities);
        let cached = cache::ThinChangeGraph::new(
            tip_oids,
            graph.schema().clone(),
            graph.schema_commit(),
            valid_history,
        );
        cache.put(spec.object_id, cached.clone())?;
        cached
    } else {
        return Err(error::Update::NoSuchObject);
    };

    cached.borrow_mut().propose_change(spec.changes.as_ref())?;

    let change = change::Change::create(
        identity_oid(&within_identity),
        author.content_id.into(),
        repo,
        signer,
        change::NewChangeSpec {
            tips: Some(cached.borrow().tips().iter().cloned().collect()),
            schema_commit: cached.borrow().schema_commit(),
            history: spec.changes,
            typename: spec.typename.clone(),
            message: spec.message,
        },
    )?;

    let previous_ref = if let Some(local) = existing_refs.local {
        Some(local.peel_to_commit()?.id())
    } else {
        None
    };

    cached
        .borrow_mut()
        .update_ref(previous_ref, *change.commit());
    cache.put(spec.object_id, cached.clone())?;

    //let new_commit = *change.commit();
    refs_storage
        .update_ref(
            &identity_urn(&within_identity),
            &spec.typename,
            spec.object_id,
            *change.commit(),
        )
        .map_err(error::Update::Refs)?;

    let new_object = CollaborativeObject {
        typename: spec.typename.clone(),
        history: cached.borrow().history(),
        id: spec.object_id,
        containing_identity: within_identity,
        schema: cached.borrow().schema().clone(),
    };

    Ok(new_object)
}

/// Retrieve addittional information about the change graph of an object. This
/// is mostly useful for debugging and testing
pub fn changegraph_info_for_object<R: RefsStorage>(
    refs_storage: &R,
    repo: &git2::Repository,
    within_identity: Either<Person, Project>,
    typename: &TypeName,
    oid: &ObjectId,
) -> Result<Option<ChangeGraphInfo>, error::Retrieve<R::Error>> {
    let tip_refs = refs_storage
        .object_references(&identity_urn(&within_identity), typename, oid)
        .map_err(error::Retrieve::Refs)?;
    if let Some(graph) = ChangeGraph::load(tip_refs.iter(), repo, &within_identity, typename, oid)?
    {
        Ok(Some(ChangeGraphInfo {
            object_id: *oid,
            dotviz: graph.graphviz(),
            number_of_nodes: graph.number_of_nodes(),
            tips: graph.tips(),
        }))
    } else {
        Ok(None)
    }
}

fn identity_urn(id: &Either<Person, Project>) -> Urn {
    id.clone()
        .map_left(|i| i.urn())
        .map_right(|i| i.urn())
        .into_inner()
}

fn identity_oid(id: &Either<Person, Project>) -> git2::Oid {
    id.as_ref()
        .map_left(|i| git2::Oid::from(i.content_id))
        .map_right(|i| git2::Oid::from(i.content_id))
        .into_inner()
}

fn open_cache<P: AsRef<std::path::Path>>(path: Option<P>) -> Box<dyn cache::Cache> {
    match path {
        Some(p) => Box::new(cache::FileSystemCache::open(p.as_ref())),
        None => Box::new(cache::NoOpCache::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::TypeName;
    use std::str::FromStr;

    #[test]
    fn test_valid_typenames() {
        assert!(TypeName::from_str("abc.def.ghi").is_ok());
        assert!(TypeName::from_str("abc.123.ghi").is_ok());
        assert!(TypeName::from_str("1bc.123.ghi").is_ok());
        assert!(TypeName::from_str(".abc.123.ghi").is_err());
        assert!(TypeName::from_str("abc.123.ghi.").is_err());
    }
}
