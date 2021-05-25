// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use super::{
    validated_automerge::{error::ProposalError, ValidatedAutomerge},
    History,
    ObjectId,
    Schema,
};

use thiserror::Error;
use tracing::instrument;

use radicle_git_ext as ext;
use serde::{Deserialize, Serialize};

use std::{cell::RefCell, collections::HashSet, convert::TryFrom, path::PathBuf, rc::Rc};

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    SchemaParse(#[from] super::schema::error::Parse),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Git(#[from] git2::Error),
}

pub trait Cache {
    fn load(
        &mut self,
        oid: ObjectId,
        known_tips: &HashSet<git2::Oid>,
    ) -> Result<Option<Rc<RefCell<ThinChangeGraph>>>, Error>;

    fn put(&mut self, oid: ObjectId, graph: Rc<RefCell<ThinChangeGraph>>) -> Result<(), Error>;
}

/// A representation of a change graph which contains only the history generated
/// by fully evaluating the change graph and the hashes of the tips of the graph
/// that was used to generate the history.
pub struct ThinChangeGraph {
    history: Option<ValidatedAutomerge>,
    raw_history: Vec<u8>,
    refs: HashSet<git2::Oid>,
    schema_commit: git2::Oid,
    schema: Schema,
}

impl ThinChangeGraph {
    pub(crate) fn new<T: Iterator<Item = git2::Oid>>(
        tips: impl IntoIterator<IntoIter = T, Item = git2::Oid>,
        schema: Schema,
        schema_commit: git2::Oid,
        history: ValidatedAutomerge,
    ) -> Rc<RefCell<ThinChangeGraph>> {
        let raw = history.compressed_valid_history().as_ref().into();
        Rc::new(RefCell::new(ThinChangeGraph {
            history: Some(history),
            raw_history: raw,
            schema,
            refs: tips.into_iter().collect(),
            schema_commit,
        }))
    }

    pub(crate) fn new_from_single_change(
        change: git2::Oid,
        schema: Schema,
        schema_commit: git2::Oid,
        history: ValidatedAutomerge,
    ) -> Rc<RefCell<ThinChangeGraph>> {
        let raw = history.compressed_valid_history().as_ref().into();
        let mut tips = HashSet::new();
        tips.insert(change);
        Rc::new(RefCell::new(ThinChangeGraph {
            history: Some(history),
            raw_history: raw,
            schema,
            refs: tips,
            schema_commit,
        }))
    }

    pub(crate) fn history(&self) -> History {
        if let Some(history) = &self.history {
            history.valid_history()
        } else {
            History::Automerge(self.raw_history.clone())
        }
    }

    pub(crate) fn propose_change(&mut self, change_bytes: &[u8]) -> Result<(), ProposalError> {
        if let Some(history) = &mut self.history {
            history.propose_change(change_bytes)?;
        } else {
            // This unwrap should be safe as we only save things in the cache when we've
            // validated them
            let mut history =
                ValidatedAutomerge::new_with_history(self.schema.clone(), self.raw_history.clone())
                    .unwrap();
            history.propose_change(change_bytes)?;
            self.history = Some(history);
        }
        Ok(())
    }

    pub(crate) fn raw(&self) -> &[u8] {
        if let Some(history) = &self.history {
            history.raw()
        } else {
            &self.raw_history
        }
    }

    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn schema_commit(&self) -> git2::Oid {
        self.schema_commit
    }

    pub(crate) fn tips(&self) -> HashSet<git2::Oid> {
        self.refs.clone()
    }

    pub(crate) fn update_ref(&mut self, previous: Option<git2::Oid>, new: git2::Oid) {
        if let Some(previous) = previous {
            self.refs.remove(&previous);
        }
        self.refs.insert(new);
    }
}

pub(crate) struct FileSystemCache {
    dir: PathBuf,
    hot_cache: lru::LruCache<ObjectId, Rc<RefCell<ThinChangeGraph>>>,
}

impl FileSystemCache {
    pub(crate) fn open<P: Into<PathBuf>>(dir: P) -> FileSystemCache {
        let dir = dir.into();
        tracing::debug!(dir=?dir, "opening cache");
        FileSystemCache {
            dir,
            hot_cache: lru::LruCache::new(100),
        }
    }

    fn object_path(&self, oid: ObjectId) -> std::path::PathBuf {
        self.dir.join(oid.to_string())
    }

    fn schema_path(&self, oid: ObjectId) -> std::path::PathBuf {
        self.object_path(oid).join("schema.json")
    }

    fn document_path(&self, oid: ObjectId) -> std::path::PathBuf {
        self.object_path(oid).join("document")
    }

    fn metadata_path(&self, oid: ObjectId) -> std::path::PathBuf {
        self.object_path(oid).join("metadata.json")
    }
}

impl Cache for FileSystemCache {
    #[instrument(level = "trace", skip(self, known_refs))]
    fn load(
        &mut self,
        oid: ObjectId,
        known_refs: &HashSet<git2::Oid>,
    ) -> Result<Option<Rc<RefCell<ThinChangeGraph>>>, Error> {
        let object_path = self.dir.join(oid.to_string());
        if !object_path.exists() {
            tracing::trace!(object_id=?oid, object_cache_path=?object_path, "no cache found on filesystem for object");
            Ok(None)
        } else {
            if self.hot_cache.contains(&oid) {
                let obj = self.hot_cache.get(&oid).unwrap().clone();
                if known_refs == &obj.borrow().refs {
                    tracing::trace!(object_id=?oid, "fresh object found in memory cache");
                    return Ok(Some(obj));
                } else {
                    tracing::trace!(fresh_refs=?known_refs, cached_refs=?obj.borrow().refs, "stale object found in memory cache");
                    self.hot_cache.pop(&oid);
                    return Ok(None);
                }
            }
            let schema_bytes = std::fs::read(self.schema_path(oid))?;
            let schema = Schema::try_from(&schema_bytes[..])?;

            let metadata_bytes = std::fs::read(self.metadata_path(oid))?;
            let metadata: Metadata = serde_json::from_slice(&metadata_bytes)?;
            let refs = metadata.refs.clone();
            let schema_commit = metadata.schema_commit;

            let contents = std::fs::read(self.document_path(oid))?;

            let thin_graph = Rc::new(RefCell::new(ThinChangeGraph {
                history: None,
                raw_history: contents,
                schema,
                refs: refs.iter().map(|i| (*i).into()).collect(),
                schema_commit: schema_commit.into(),
            }));

            if known_refs == &thin_graph.borrow().refs {
                tracing::trace!(object_id=?oid, "fresh object found in filesystem cache");
                self.hot_cache.put(oid, thin_graph.clone());
                Ok(Some(thin_graph))
            } else {
                tracing::trace!(fresh_refs=?known_refs, cached_refs=?thin_graph.borrow().refs, "stale object found in filesystem cache");
                Ok(None)
            }
        }
    }

    #[instrument(level = "trace", skip(self, graph))]
    fn put(&mut self, oid: ObjectId, graph: Rc<RefCell<ThinChangeGraph>>) -> Result<(), Error> {
        if !self.object_path(oid).exists() {
            std::fs::create_dir_all(self.object_path(oid))?;
        }
        let schema_bytes = graph.borrow().schema().json_bytes();
        std::fs::write(self.schema_path(oid), &schema_bytes)?;

        let metadata = Metadata {
            refs: graph.borrow().refs.iter().map(|i| (*i).into()).collect(),
            schema_commit: graph.borrow().schema_commit.into(),
        };

        let metadata_bytes = serde_json::to_vec(&metadata)?;
        std::fs::write(self.metadata_path(oid), &metadata_bytes)?;

        std::fs::write(self.document_path(oid), graph.borrow().raw())?;
        self.hot_cache.put(oid, graph);
        Ok(())
    }
}

pub struct NoOpCache {}

impl NoOpCache {
    pub fn new() -> NoOpCache {
        NoOpCache {}
    }
}

impl Cache for NoOpCache {
    fn put(
        &mut self,
        _oid: ObjectId,
        _graph: Rc<RefCell<ThinChangeGraph>>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn load(
        &mut self,
        _oid: ObjectId,
        _known_tips: &HashSet<git2::Oid>,
    ) -> Result<Option<Rc<RefCell<ThinChangeGraph>>>, Error> {
        Ok(None)
    }
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    refs: HashSet<ext::Oid>,
    schema_commit: ext::Oid,
}
