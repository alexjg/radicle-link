// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use super::{
    schema_change,
    validated_automerge::ValidatedAutomerge,
    Change,
    CollaborativeObject,
    History,
    IdentityCache,
    ObjectId,
    Schema,
    SchemaChange,
    TypeName,
};
use either::Either;
use link_crypto::PublicKey;
use link_identities::git::{Person, Project};
use thiserror::Error as ThisError;

use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap, HashSet},
    convert::TryInto,
};

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Missing commit: {0}")]
    MissingRevision(git2::Oid),
    #[error(transparent)]
    Git(#[from] git2::Error),
    #[error(transparent)]
    LoadSchema(#[from] schema_change::error::Load),
}

/// The graph of hanges for a particular collaborative object
pub(super) struct ChangeGraph {
    object_id: ObjectId,
    containing_identity: Either<Person, Project>,
    graph: petgraph::Graph<Change, ()>,
    schema_change: SchemaChange,
}

impl ChangeGraph {
    /// Load the change graph from the underlying git store by walking
    /// backwards from references to the object
    #[tracing::instrument(skip(repo, tip_refs, containing_identity))]
    pub(super) fn load<'a>(
        tip_refs: impl Iterator<Item = &'a git2::Reference<'a>>,
        repo: &git2::Repository,
        containing_identity: &Either<Person, Project>,
        typename: &TypeName,
        oid: &ObjectId,
    ) -> Result<Option<ChangeGraph>, Error> {
        let mut builder = GraphBuilder::default();
        let mut edges_to_process: Vec<(git2::Commit, git2::Oid)> = Vec::new();
        let tip_refs: Vec<&git2::Reference<'_>> = tip_refs.collect();
        let ref_names: Vec<&str> = tip_refs.iter().filter_map(|r| r.name()).collect();
        tracing::trace!(refs=?ref_names, "loading object from references");

        // Populate the initial set of edges_to_process from the refs we have
        for reference in tip_refs {
            let commit = reference.peel_to_commit()?;
            match Change::load(repo, &commit) {
                Ok(change) => {
                    let author_commit = *change.author_commit();
                    let schema_commit = *change.schema_commit();
                    let containing_identity_commit = change.containing_identity_commit();
                    builder.add_change(change);
                    for parent in commit.parents() {
                        if parent.id() != author_commit
                            && parent.id() != schema_commit
                            && parent.id() != containing_identity_commit
                            && !builder.has_edge(parent.id(), commit.id()) {
                                edges_to_process.push((parent, commit.id()));
                        }
                    }
                },
                Err(e) => {
                    tracing::warn!(err=?e, commit=?commit.id(), reference=?reference.name(), "unable to load change from reference");
                },
            }
        }

        // Process edges until we have no more to process
        while let Some((parent_commit, child_commit_id)) = edges_to_process.pop() {
            tracing::trace!(?parent_commit, ?child_commit_id, "loading change");
            match Change::load(repo, &parent_commit) {
                Ok(change) => {
                    let author_commit = *change.author_commit();
                    let schema_commit = *change.schema_commit();
                    let containing_identity_commit = change.containing_identity_commit();
                    builder.add_change(change);
                    builder.add_edge(child_commit_id, parent_commit.id());
                    for grand_parent in parent_commit.parents() {
                        if grand_parent.id() != author_commit
                            && grand_parent.id() != schema_commit
                            && grand_parent.id() != containing_identity_commit
                            && !builder.has_edge(grand_parent.id(), parent_commit.id()) {
                                edges_to_process.push((grand_parent, parent_commit.id()));
                        }
                    }
                },
                Err(e) => {
                    tracing::warn!(err=?e, commit=?parent_commit.id(), "unable to load changetree from commit");
                },
            }
        }
        builder.build(repo, *oid, containing_identity.clone())
    }

    /// Given a graph evaluate it to produce a collaborative object. This will
    /// filter out branches of the graph which do not have valid signatures,
    /// or which do not have permission to make a change, or which make a
    /// change which invalidates the schema of the object
    pub(super) fn evaluate(
        &self,
        identities: &mut IdentityCache,
    ) -> (CollaborativeObject, ValidatedAutomerge) {
        let mut roots: Vec<petgraph::graph::NodeIndex<u32>> = self
            .graph
            .externals(petgraph::Direction::Incoming)
            .collect();
        roots.sort();
        let mut valid_automerge = ValidatedAutomerge::new(self.schema_change.schema().clone());
        // This is okay because we check that the graph has a root node in
        // GraphBuilder::build
        let root = roots.first().unwrap();
        let typename = {
            let first_node = &self.graph[*root];
            first_node.typename().clone()
        };
        petgraph::visit::depth_first_search(&self.graph, vec![*root], |event| {
            if let petgraph::visit::DfsEvent::Discover(n, _) = event {
                let change = &self.graph[n];
                let containing_identity = match identities
                    .lookup_identity(change.containing_identity_commit())
                {
                    Ok(Some(id)) => id.clone(),
                    _ => {
                        tracing::warn!(commit=?change.commit(), "rejecting change which referenced an invalid containing identity");
                        return petgraph::visit::Control::Prune;
                    },
                };
                if !are_same_identity(&containing_identity, &self.containing_identity) {
                    tracing::warn!(commit=?change.commit(), "rejecting change which says it's for a containing identity it's ref does not point at");
                    return petgraph::visit::Control::Prune;
                }
                if !change.valid_signatures() {
                    tracing::warn!(commit=?change.commit(), "invalid signature");
                    return petgraph::visit::Control::Prune;
                }
                match identities
                    .lookup_identity(*change.author_commit())
                    .ok()
                    .flatten()
                {
                    Some(Either::Left(author)) => {
                        match containing_identity {
                            Either::Left(p) => {
                                if p.urn() != author.urn() {
                                    tracing::warn!(change_commit=?change.commit(), "rejecting change for person object because it is authored by a different identity");
                                    return petgraph::visit::Control::Prune;
                                }
                            },
                            Either::Right(p) => {
                                if !is_maintainer(&p, author) {
                                    tracing::warn!(change_commit=?change.commit(), "rejecting change from non-maintainer");
                                    return petgraph::visit::Control::Prune;
                                }
                            },
                        };
                    },
                    Some(Either::Right(_)) => {
                        tracing::warn!(change_commit=?change.commit(), "rejecting change which was signed by a project identity");
                        return petgraph::visit::Control::Prune;
                    },
                    None => {
                        tracing::warn!(change_commit=?change.commit(), author_commit=?change.author_commit(), "could not find author for change",);
                        return petgraph::visit::Control::Prune;
                    },
                };
                match &change.history() {
                    History::Automerge(bytes) => match valid_automerge.propose_change(bytes) {
                        Ok(()) => {},
                        Err(e) => {
                            tracing::warn!(commit=?change.commit(), err=?e, "error applying change");
                            return petgraph::visit::Control::Prune;
                        },
                    },
                };
            };
            petgraph::visit::Control::Continue::<()>
        });
        (
            CollaborativeObject {
                containing_identity: self.containing_identity.clone(),
                typename,
                history: valid_automerge.valid_history(),
                id: self.object_id,
                schema: self.schema_change.schema().clone(),
            },
            valid_automerge,
        )
    }

    /// Get the tips of the collaborative object
    pub(super) fn tips(&self) -> HashSet<git2::Oid> {
        self.graph
            .externals(petgraph::Direction::Outgoing)
            .map(|n| {
                let change = &self.graph[n];
                *change.commit()
            })
            .collect()
    }

    pub(super) fn number_of_nodes(&self) -> u64 {
        self.graph.node_count().try_into().unwrap()
    }

    pub(super) fn graphviz(&self) -> String {
        let for_display = self.graph.map(|_ix, n| n.to_string(), |_ix, _e| "");
        petgraph::dot::Dot::new(&for_display).to_string()
    }

    pub(super) fn schema_commit(&self) -> git2::Oid {
        self.schema_change.commit()
    }

    pub(super) fn schema(&self) -> &Schema {
        self.schema_change.schema()
    }
}

struct GraphBuilder {
    node_indices: HashMap<git2::Oid, petgraph::graph::NodeIndex<u32>>,
    graph: petgraph::Graph<Change, ()>,
}

impl Default for GraphBuilder {
    fn default() -> Self {
        GraphBuilder {
            node_indices: HashMap::new(),
            graph: petgraph::graph::Graph::new(),
        }
    }
}

impl GraphBuilder {
    fn add_change(&mut self, change: Change) {
        let commit = *change.commit();
        if let Entry::Vacant(e) = self.node_indices.entry(commit) {
            let ix = self.graph.add_node(change);
            e.insert(ix);
        }
    }

    fn has_edge(&mut self, parent_id: git2::Oid, child_id: git2::Oid) -> bool {
        let parent_ix = self.node_indices.get(&parent_id);
        let child_ix = self.node_indices.get(&child_id);
        match (parent_ix, child_ix) {
            (Some(parent_ix), Some(child_ix)) => self.graph.contains_edge(*parent_ix, *child_ix),
            _ => false,
        }
    }

    fn add_edge(&mut self, child: git2::Oid, parent: git2::Oid) {
        // This panics if the child or parent ids are not in the graph already
        let child_id = self.node_indices.get(&child).unwrap();
        let parent_id = self.node_indices.get(&parent).unwrap();
        self.graph.update_edge(*parent_id, *child_id, ());
    }

    fn build(
        self,
        repo: &git2::Repository,
        object_id: ObjectId,
        within_identity: Either<Person, Project>,
    ) -> Result<Option<ChangeGraph>, Error> {
        if let Some(root) = self.graph.externals(petgraph::Direction::Incoming).next() {
            let root_change = &self.graph[root];
            let schema_change = SchemaChange::load(*root_change.schema_commit(), repo)?;
            Ok(Some(ChangeGraph {
                schema_change,
                object_id,
                containing_identity: within_identity,
                graph: self.graph,
            }))
        } else {
            Ok(None)
        }
    }
}

fn is_maintainer(project: &Project, person: &Person) -> bool {
    let keys: BTreeSet<&PublicKey> = person.delegations().iter().collect();
    project
        .delegations()
        .eligible(keys)
        .ok()
        .map(|k| !k.is_empty())
        .unwrap_or(false)
}

fn are_same_identity(left: &Either<Person, Project>, right: &Either<Person, Project>) -> bool {
    let left_urn = left
        .as_ref()
        .map_left(|i| i.urn())
        .map_right(|i| i.urn())
        .into_inner();
    let right_urn = right
        .as_ref()
        .map_right(|i| i.urn())
        .map_left(|i| i.urn())
        .into_inner();
    left_urn == right_urn
}
