use std::{collections::BTreeSet, convert::TryFrom, fmt::Debug};

use crate::{
    git::{
        storage::{self, ReadOnlyStorage},
        Urn,
    },
    identities::git::VerifiedProject,
    PeerId,
};
use git_ext::RefLike;
use git_ref_format::{lit, name, Namespaced, Qualified, RefStr, RefString};

#[derive(Clone, Debug, PartialEq)]
pub enum DefaultBranchHead {
    /// Not all delegates agreed on an ancestry tree. Each set of diverging
    /// delegates is included as a `Fork`
    Forked(BTreeSet<Fork>),
    /// All the delegates agreed on an ancestry tree
    Head {
        /// The most recent commit for the tree
        target: git2::Oid,
        /// The branch name which is the default branch
        branch: RefString,
    },
}

#[derive(Clone, Debug, std::hash::Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fork {
    /// Peers which are in the ancestry set of this fork but not the tips. This
    /// means that these peers can appear in multiple forks
    pub ancestor_peers: BTreeSet<PeerId>,
    /// The peers pointing at the tip of this fork
    pub tip_peers: BTreeSet<PeerId>,
    /// The most recent tip
    pub tip: git2::Oid,
}

pub mod error {
    use git_ref_format as ref_format;
    use std::collections::BTreeSet;

    use crate::git::storage::read;

    #[derive(thiserror::Error, Debug)]
    pub enum FindDefaultBranch {
        #[error("the project payload does not define a default branch")]
        NoDefaultBranch,
        #[error("no peers had published anything for the default branch")]
        NoTips,
        #[error(transparent)]
        RefFormat(#[from] ref_format::Error),
        #[error(transparent)]
        Read(#[from] read::Error),
    }

    #[derive(thiserror::Error, Debug)]
    pub enum SetDefaultBranch {
        #[error(transparent)]
        Find(#[from] FindDefaultBranch),
        #[error(transparent)]
        Git(#[from] git2::Error),
        #[error("the delegates have forked")]
        Forked(BTreeSet<super::Fork>),
    }
}

/// Find the head of the default branch of `project`
///
/// In general there can be a different view of the default branch of a project
/// for each peer ID of each delegate and there is no reason that these would
/// all be compatible. It's quite possible that two peers publish entirely
/// unrelated ancestry trees for a given branch. In this case this function will
/// return [`DefaultBranchHead::Forked`].
///
/// However, often it's the case that delegates do agree on an ancestry tree for
/// a particular branch and the difference between peers is just that some are
/// ahead of others. In this case this function will return
/// [`DefaultBranchHead::Head`].
///
/// # Errors
///
/// * If the project contains no default branch definition
/// * No peers had published anything for the default branch
pub fn default_branch_head(
    storage: &storage::Storage,
    project: VerifiedProject,
) -> Result<DefaultBranchHead, error::FindDefaultBranch> {
    if let Some(default_branch) = &project.payload().subject.default_branch {
        let local = storage.peer_id();
        let branch_refstring = RefString::try_from(default_branch.to_string())?;
        let mut multiverse = Multiverse::new(branch_refstring.clone());
        let peers =
            project
                .delegations()
                .into_iter()
                .flat_map(|d| -> Box<dyn Iterator<Item = PeerId>> {
                    use either::Either::*;
                    match d {
                        Left(key) => Box::new(std::iter::once(PeerId::from(*key))),
                        Right(person) => Box::new(
                            person
                                .delegations()
                                .into_iter()
                                .map(|key| PeerId::from(*key)),
                        ),
                    }
                });
        for peer_id in peers {
            let tip = peer_commit(storage, project.urn(), peer_id, local, &branch_refstring)?;
            if let Some(tip) = tip {
                multiverse.add_peer(storage, peer_id, tip)?;
            } else {
                tracing::warn!(%peer_id, %default_branch, "no default branch commit found for peer");
            }
        }
        multiverse.finish()
    } else {
        Err(error::FindDefaultBranch::NoDefaultBranch)
    }
}

/// Determine the default branch for a project and set the local HEAD to this
/// branch
///
/// In more detail, this function determines the local head using
/// [`default_branch_head`] and then sets the following references to the
/// `DefaultBranchHead::target` returned:
///
/// * `refs/namespaces/<URN>/refs/HEAD`
/// * `refs/namespaces/<URN>/refs/<default branch name>`
///
/// # Why do this?
///
/// When cloning from a namespace representing a project to a working copy we
/// would like, if possible, to omit the specification of which particular peer
/// we want to clone. Specifically we would like to clone
/// `refs/namespaces/<URN>/`. This does work, but the working copy we end up
/// with does not have any contents because git uses `refs/HEAD` of the source
/// repository to figure out what branch to set the new working copy to.
/// Therefore, by setting `refs/HEAD` and `refs/<default branch name>` of the
/// namespace `git clone` (and any other clone based workflows) does something
/// sensible and we end up with a working copy which is looking at the default
/// branch of the project.
///
/// # Errors
///
/// * If no default branch could be determined
pub fn set_default_head(
    storage: &storage::Storage,
    project: VerifiedProject,
) -> Result<git2::Oid, error::SetDefaultBranch> {
    let urn = project.urn();
    let default_head = default_branch_head(storage, project)?;
    match default_head {
        DefaultBranchHead::Head { target, branch } => {
            // Note that we can't use `Namespaced` because `refs/HEAD` is not a `Qualified`
            let head =
                RefString::try_from(format!("refs/namespaces/{}/refs/HEAD", urn.encode_id()))
                    .expect("urn is valid namespace");
            let branch_head = Namespaced::from(lit::refs_namespaces(
                &urn,
                Qualified::from(lit::refs_heads(branch)),
            ));

            let repo = storage.as_raw();
            repo.reference(
                &branch_head.clone().into_qualified(),
                target,
                true,
                "set default branch head",
            )?;
            repo.reference_symbolic(head.as_str(), branch_head.as_str(), true, "set head")?;
            Ok(target)
        },
        DefaultBranchHead::Forked(forks) => Err(error::SetDefaultBranch::Forked(forks)),
    }
}

fn peer_commit(
    storage: &storage::Storage,
    urn: Urn,
    peer_id: PeerId,
    local: &PeerId,
    branch: &RefStr,
) -> Result<Option<git2::Oid>, error::FindDefaultBranch> {
    let remote_name = RefString::try_from(peer_id.default_encoding())?;
    let reference = if local == &peer_id {
        RefString::from(Qualified::from(lit::refs_heads(branch)))
    } else {
        RefString::from(Qualified::from(lit::refs_remotes(remote_name)))
            .join(name::HEADS)
            .join(branch)
    };
    let urn = urn.with_path(Some(RefLike::from(reference)));
    let tip = storage.tip(&urn, git2::ObjectType::Commit)?;
    Ok(tip.map(|c| c.id()))
}

#[derive(Debug)]
struct Multiverse {
    branch: RefString,
    histories: Vec<History>,
}

impl Multiverse {
    fn new(branch: RefString) -> Multiverse {
        Multiverse {
            branch,
            histories: Vec::new(),
        }
    }

    fn add_peer(
        &mut self,
        storage: &storage::Storage,
        peer: PeerId,
        tip: git2::Oid,
    ) -> Result<(), error::FindDefaultBranch> {
        // If this peers tip is in the ancestors of any existing histories then we just
        // add the peer to those histories
        let mut found_descendant = false;
        for history in &mut self.histories {
            if history.ancestors.contains(&tip) {
                found_descendant = true;
                history.ancestor_peers.insert(peer);
            } else if history.tip == tip {
                found_descendant = true;
                history.tip_peers.insert(peer);
            }
        }
        if found_descendant {
            return Ok(());
        }

        // Otherwise we load a new history
        let mut history = History::load(storage, peer, tip)?;

        // Then we go through existing histories and check if any of them are ancestors
        // of the new history. If they are then we incorporate them as ancestors
        // of the new history and remove them from the multiverse
        let mut i = 0;
        while i < self.histories.len() {
            let other_history = &self.histories[i];
            if history.ancestors.contains(&other_history.tip) {
                let other_history = self.histories.remove(i);
                history.ancestor_peers.extend(other_history.ancestor_peers);
                history.ancestor_peers.extend(other_history.tip_peers);
            } else {
                i += 1;
            }
        }
        self.histories.push(history);

        Ok(())
    }

    fn finish(self) -> Result<DefaultBranchHead, error::FindDefaultBranch> {
        if self.histories.is_empty() {
            Err(error::FindDefaultBranch::NoTips)
        } else if self.histories.len() == 1 {
            Ok(DefaultBranchHead::Head {
                target: self.histories[0].tip,
                branch: self.branch,
            })
        } else {
            Ok(DefaultBranchHead::Forked(
                self.histories
                    .into_iter()
                    .map(|h| Fork {
                        ancestor_peers: h.ancestor_peers,
                        tip_peers: h.tip_peers,
                        tip: h.tip,
                    })
                    .collect(),
            ))
        }
    }
}

#[derive(Debug)]
struct History {
    tip: git2::Oid,
    tip_peers: BTreeSet<PeerId>,
    ancestor_peers: BTreeSet<PeerId>,
    ancestors: BTreeSet<git2::Oid>,
}

impl History {
    fn load(
        storage: &storage::Storage,
        peer: PeerId,
        tip: git2::Oid,
    ) -> Result<Self, storage::Error> {
        let repo = storage.as_raw();
        let mut walk = repo.revwalk()?;
        walk.set_sorting(git2::Sort::TOPOLOGICAL)?;
        walk.push(tip)?;
        let mut ancestors = walk.collect::<Result<BTreeSet<git2::Oid>, _>>()?;
        ancestors.remove(&tip);
        let mut peers = BTreeSet::new();
        peers.insert(peer);
        let mut tip_peers = BTreeSet::new();
        tip_peers.insert(peer);
        Ok(Self {
            tip,
            tip_peers,
            ancestors,
            ancestor_peers: BTreeSet::new(),
        })
    }
}
