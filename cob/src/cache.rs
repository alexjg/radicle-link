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

use std::{
    cell::RefCell,
    collections::HashSet,
    convert::TryFrom,
    path::PathBuf,
    rc::Rc,
    time::Duration,
};

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
    /// Load an object from the cache. `known_refs` are the OIDs pointed to by
    /// references to the object that we know about. If these OIDs have not
    /// changed then we may reuse the cached object. This means that if
    /// additional changes are added to the change graph (via replication or
    /// some direct twiddling of the storage) but no references to the object
    /// are changed then we will not see those changes. However, we specify
    /// in the RFC that any peer updating a change must update their ref to
    /// the object, so this should not be a problem.
    ///
    /// We return an `Rc<RefCell<ThinChangeGraph>>`. This is so that changes can
    /// be made by calling `ThinChangeGraph::propose_change`, which mutates
    /// the `ThinChangeGraph`. This allows the `ThinChangeGraph` (via it's
    /// `validated_automerge`) to cache the `automerge::Backend` and
    /// `automerge::Frontend` used to validate changes. This in turn means that
    /// we avoid rebuilding the automerge document from scratch for every
    /// change - instead we just have to rebuild in the case of schema
    /// invalidating changes, which are hopefully rare.
    fn load(
        &mut self,
        oid: ObjectId,
        known_refs: &HashSet<git2::Oid>,
    ) -> Result<Option<Rc<RefCell<ThinChangeGraph>>>, Error>;

    /// Insert or update an object in the cache
    fn put(&mut self, oid: ObjectId, graph: Rc<RefCell<ThinChangeGraph>>) -> Result<(), Error>;
}

/// A representation of a change graph which contains only the history generated
/// by fully evaluating the change graph and the OIDs that were pointed at by
/// known references to the object that were used to load the change graph.
pub struct ThinChangeGraph {
    // This is an `Option` because often we never actually need to evaluate the automerge document
    // at all. If we are loading objects from the cache to return in response to a read request
    // then we already know that the document is valid (otherwise it would never be in the
    // cache in the first place) and we can just return the raw history we read from the cache.
    // When we do need the full history (e.g when we need to make an update and therefore need
    // to validate the update with respect to the schema) then we generate the
    // `ValidatedAutomerge` from the `raw_history`.
    history: Option<ValidatedAutomerge>,
    raw_history: Vec<u8>,
    refs: HashSet<git2::Oid>,
    schema_commit: git2::Oid,
    schema: Schema,
    state: serde_json::Value,
}

impl PartialEq for ThinChangeGraph {
    fn eq(&self, other: &Self) -> bool {
        self.raw_history == other.raw_history
            && self.schema_commit == other.schema_commit
            && self.refs == other.refs
            && self.schema == other.schema
            && self.state == other.state
    }
}

impl Clone for ThinChangeGraph {
    fn clone(&self) -> Self {
        ThinChangeGraph {
            history: None,
            raw_history: self.raw_history.clone(),
            refs: self.refs.clone(),
            schema_commit: self.schema_commit,
            schema: self.schema.clone(),
            state: self.state.clone(),
        }
    }
}

impl ThinChangeGraph {
    pub(crate) fn new<T: Iterator<Item = git2::Oid>>(
        tips: impl IntoIterator<IntoIter = T, Item = git2::Oid>,
        schema: Schema,
        schema_commit: git2::Oid,
        history: ValidatedAutomerge,
    ) -> Rc<RefCell<ThinChangeGraph>> {
        let raw = history.compressed_valid_history().as_ref().into();
        let state = history.state();
        let g = ThinChangeGraph {
            history: Some(history),
            raw_history: raw,
            schema,
            refs: tips.into_iter().collect(),
            schema_commit,
            state,
        };
        Rc::new(RefCell::new(g))
    }

    pub(crate) fn new_from_single_change(
        change: git2::Oid,
        schema: Schema,
        schema_commit: git2::Oid,
        history: ValidatedAutomerge,
    ) -> Rc<RefCell<ThinChangeGraph>> {
        let raw = history.compressed_valid_history().as_ref().into();
        let mut tips = HashSet::new();
        let state = history.state();
        tips.insert(change);
        Rc::new(RefCell::new(ThinChangeGraph {
            history: Some(history),
            raw_history: raw,
            schema,
            refs: tips,
            schema_commit,
            state,
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

#[derive(Serialize, Deserialize)]
struct Metadata {
    refs: HashSet<ext::Oid>,
    schema_commit: ext::Oid,
}

/// A cache which stores it's objects on the file system. A sort of poor mans
/// database, this cache is designed to be safe for use from concurrent
/// processes and to be easy to upgrade. The layout on disk looks like this:
///
/// ```ignore
/// <cache dir>
/// |- v1
/// |  |- <object 1 id>
/// |  |  |- schema.json
/// |  |  |- document
/// |  |  |- metadata.json
/// |  |  |- state.json
/// |  |- <object 1 id>.lock
/// |  |- <object id>
/// |  |  ...
/// ```
///
/// - `schema.json` contains the JSON schema that has been calculated for this
///   object.
/// - `document` contains the saved automerge document for all valid changes
/// - `metadata.json` contains the OIDs of the refs used to generate the cached
///   object, as well as OID of the commit which contains the schema change of
///   the object.
/// - `state.json` contains a JSON representation of the state of the automerge
///   document
///
/// The `<object 1 id>.lock` file is created when reading from or writing to an
/// object, which makes is safe to run multiple processes accessing the cache at
/// once.
///
/// The `v1` directory means we can easily add a `v2` if we need to change the
/// cache layout in backwards incompatible ways.
///
/// There are a number of ways the cache can get into an invalid state. As you
/// might expect these ways are to do with the caching process crashing in the
/// middle of some disk operation.
///
/// - The lockfile could be created and then the process crashes without
///   removing the cache. This will result in an error being thrown in
///   consequent processes as they fail to acquire a lock on the object. This
///   could be fixed by manually removing the lockfile.
/// - The caching process could crash part way through writing an object. In
///   this case the lockfile would still exist so the error would manifest as a
///   failure to acquire the lock. But manually removing the lockfile could then
///   result in errors as the cached data could be incomplete.
///
/// We could write some tooling to "fix" broken caches which would do something
/// like yell at the user to make sure they've killed all processes which might
/// access the cache and then delete any object which has a lockfile.
/// Alternatively, we could use a proper database™.
pub(crate) struct FileSystemCache {
    dir: PathBuf,
    /// An in memory cache of the last 100 objects that were loaded. This is
    /// useful for situations where you're iteratively applying updates -
    /// one after another - to the same object because it avoids hitting the
    /// disk for every update.
    hot_cache: lru::LruCache<ObjectId, Rc<RefCell<ThinChangeGraph>>>,
}

impl FileSystemCache {
    pub(crate) fn open<P: Into<PathBuf>>(dir: P) -> Result<FileSystemCache, std::io::Error> {
        // We add a version to the path so that we can support multiple incompatible
        // cache versions at the same time.
        let dir = dir.into().join("v1");
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        tracing::debug!(dir=?dir, "opening cache");
        Ok(FileSystemCache {
            dir,
            hot_cache: lru::LruCache::new(100),
        })
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

    fn object_lockfile_path(&self, oid: ObjectId) -> std::path::PathBuf {
        let lockfile_name = format!("{}.lock", oid);
        self.dir.join(lockfile_name)
    }

    fn state_path(&self, oid: ObjectId) -> std::path::PathBuf {
        self.object_path(oid).join("state.json")
    }

    fn acquire_lock(&mut self, object_id: ObjectId) -> Result<LockFile, std::io::Error> {
        let lockfile_path = self.object_lockfile_path(object_id);
        let mut attempts = 0;
        loop {
            if let Some(lockfile) = LockFile::create(&lockfile_path)? {
                tracing::trace!(object_id=?object_id, path=?lockfile_path, "acquired lockfile");
                return Ok(lockfile);
            } else {
                attempts += 1;
                if attempts > 20 {
                    tracing::error!(lockfile=?lockfile_path, attempts=?attempts, "unable to obtain lockfile");
                    let message = format!("unable to obtain lockfile: {:?}", lockfile_path);
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, message));
                }
                tracing::trace!(object_id=?object_id, path=?lockfile_path, "waiting for lockfile");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

impl Cache for FileSystemCache {
    #[instrument(level = "trace", skip(self, known_refs))]
    fn load(
        &mut self,
        oid: ObjectId,
        known_refs: &HashSet<git2::Oid>,
    ) -> Result<Option<Rc<RefCell<ThinChangeGraph>>>, Error> {
        let _lockfile = self.acquire_lock(oid)?;
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

            let state_bytes = std::fs::read(self.state_path(oid))?;
            let state: serde_json::Value = serde_json::from_slice(&state_bytes)?;

            let thin_graph = Rc::new(RefCell::new(ThinChangeGraph {
                history: None,
                raw_history: contents,
                schema,
                refs: refs.iter().map(|i| (*i).into()).collect(),
                schema_commit: schema_commit.into(),
                state,
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
        let _lockfile = self.acquire_lock(oid);
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

        let state_bytes = serde_json::to_vec(&graph.borrow().state)?;
        std::fs::write(self.state_path(oid), &state_bytes)?;

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
    fn put(&mut self, _oid: ObjectId, _graph: Rc<RefCell<ThinChangeGraph>>) -> Result<(), Error> {
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

#[derive(Debug)]
pub struct LockFile {
    handle: Option<std::fs::File>,
    path: PathBuf,
}

impl LockFile {
    /// Create a lockfile at the given path.
    ///
    /// If the lockfile does not already exist this will create the file and
    /// return Some(LockFile), otherwise it will return None
    ///
    /// # Panics
    ///
    /// Will panic if the path doesn't have a parent directory.
    pub fn create(path: impl AsRef<std::path::Path>) -> Result<Option<LockFile>, std::io::Error> {
        let path = path.as_ref();

        path.parent().expect("lockfile path must have a parent");

        let mut openopts = std::fs::OpenOptions::new();
        openopts.create_new(true).read(true).write(true);
        match openopts.open(path) {
            Ok(f) => Ok(Some(LockFile {
                handle: Some(f),
                path: path.to_owned(),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            drop(handle);

            match std::fs::remove_file(&self.path) {
                Ok(()) => tracing::debug!(path=?self.path, "removed lockfile"),
                Err(e) => tracing::warn!(err=?e, path=?self.path,"could not remove lockfile"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Cache, FileSystemCache, ThinChangeGraph};
    use crate::{ObjectId, Schema};
    use lazy_static::lazy_static;
    use rand::{seq::IteratorRandom, Rng};
    use std::{
        cell::RefCell,
        collections::HashSet,
        convert::{TryFrom, TryInto},
        env::temp_dir,
        rc::Rc,
    };

    lazy_static! {
        static ref SCHEMA: Schema = Schema::try_from(&serde_json::json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" }
            },
            "required": ["name"]
        }))
        .unwrap();
    }

    #[test]
    fn smash_the_cache() {
        env_logger::builder().is_test(true).init();
        // On each thread create a cache pointing at the same directory. Each
        // thread stores a different object state under the same object
        // ID. Now repeatedly store and load objects from the object on
        // each thread. Each thread will see different states on each load but
        // they should never see a mixture of states.
        let states: [&str; 3] = ["one", "two", "three"];
        let graph_states: Vec<ObjectState> = states.iter().map(|s| object_state(s)).collect();

        let cache_dirname: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();
        let cache_dir = temp_dir().join(cache_dirname);

        let the_oid: ObjectId = random_oid().into();

        let mut threads = Vec::new();
        // Writer threads
        for _ in 0..3 {
            let cache_dir = cache_dir.clone();
            let graph_states = graph_states.clone();
            threads.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let mut cache = FileSystemCache::open(cache_dir.as_path()).unwrap();
                    let thin_graph = Rc::new(RefCell::new(
                        graph_states
                            .iter()
                            .choose(&mut rand::thread_rng())
                            .unwrap()
                            .into(),
                    ));
                    cache.put(the_oid, thin_graph.clone()).unwrap();
                }
            }));
        }

        // reader threads
        for _ in 0..10 {
            let cache_dir = cache_dir.clone();
            let graph_states = graph_states.clone();
            let mut successful_loads = 0;
            threads.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let mut cache = FileSystemCache::open(cache_dir.as_path()).unwrap();
                    for state in &graph_states {
                        if let Some(g) = cache.load(the_oid, &state.refs).unwrap() {
                            let objstate: ObjectState = g.into();
                            successful_loads += 1;
                            assert!(graph_states.contains(&objstate));
                        }
                    }
                }
                assert!(successful_loads > 0);
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }
    }

    /// The same as a ThinChangeGraph, just without the ValidatedAutomerge
    /// (which is not`Send`) so that it can be easily sent between threads
    /// for the purposes of comparison
    #[derive(PartialEq, Clone)]
    struct ObjectState {
        raw_history: Vec<u8>,
        refs: HashSet<git2::Oid>,
        schema_commit: git2::Oid,
        schema: Schema,
        state: serde_json::Value,
    }

    impl<A: AsRef<ThinChangeGraph>> From<A> for ObjectState {
        fn from(g: A) -> Self {
            ObjectState {
                raw_history: g.as_ref().raw_history.clone(),
                refs: g.as_ref().refs.clone(),
                schema_commit: g.as_ref().schema_commit,
                schema: g.as_ref().schema.clone(),
                state: g.as_ref().state.clone(),
            }
        }
    }

    impl From<Rc<RefCell<ThinChangeGraph>>> for ObjectState {
        fn from(g: Rc<RefCell<ThinChangeGraph>>) -> Self {
            ObjectState {
                raw_history: g.borrow().raw_history.clone(),
                refs: g.borrow().refs.clone(),
                schema_commit: g.borrow().schema_commit,
                schema: g.borrow().schema.clone(),
                state: g.borrow().state.clone(),
            }
        }
    }

    impl From<&ObjectState> for ThinChangeGraph {
        fn from(o: &ObjectState) -> Self {
            ThinChangeGraph {
                history: None,
                raw_history: o.raw_history.clone(),
                refs: o.refs.clone(),
                schema_commit: o.schema_commit,
                schema: o.schema.clone(),
                state: o.state.clone(),
            }
        }
    }

    fn object_state(name: &'static str) -> ObjectState {
        let tips = [0..10].iter().map(|_| random_oid());
        let schema_commit = random_oid();
        let (history, state) = history(name);
        ObjectState {
            raw_history: history,
            refs: tips.collect(),
            schema_commit,
            schema: SCHEMA.clone(),
            state,
        }
    }

    fn history(name: &'static str) -> (Vec<u8>, serde_json::Value) {
        let mut backend = automerge::Backend::new();
        let mut frontend = automerge::Frontend::new();
        let (_, change) = frontend
            .change::<_, _, automerge::InvalidChangeRequest>(None, |d| {
                d.add_change(automerge::LocalChange::set(
                    automerge::Path::root().key("name"),
                    automerge::Value::Primitive(automerge::Primitive::Str(name.into())),
                ))?;
                Ok(())
            })
            .unwrap();
        backend.apply_local_change(change.unwrap()).unwrap();
        let history = backend.save().unwrap();
        let state = frontend
            .get_value(&automerge::Path::root())
            .unwrap()
            .to_json();
        (history, state)
    }

    fn random_oid() -> git2::Oid {
        let oid_raw: [u8; 20] = rand::random();
        git2::Oid::from_bytes(&oid_raw).unwrap()
    }

    /// This test checks that we can load a cached object from a test fixture.
    /// The intention is to guard against future changes to the layout of
    /// cache files which would make existing caches unloadable.
    #[test]
    fn test_load_v1() {
        let fixture_path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("fixtures")
            .join("cache");
        let tip: radicle_git_ext::Oid = "79312fa994c92c7467bfbc03e96aa0412cffa267"
            .try_into()
            .unwrap();
        let mut tips: HashSet<git2::Oid> = HashSet::new();
        tips.insert(tip.into());
        let mut cache = FileSystemCache::open(fixture_path).unwrap();
        assert!(cache
            .load(
                radicle_git_ext::Oid::try_from("79312fa994c92c7467bfbc03e96aa0412cffa267")
                    .unwrap()
                    .into(),
                &tips,
            )
            .unwrap()
            .is_some());
    }
}
