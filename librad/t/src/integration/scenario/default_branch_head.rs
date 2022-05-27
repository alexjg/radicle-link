// Copyright © 2019-2020 The Radicle Foundation <hello@radicle.foundation>
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{convert::TryFrom, ops::Index as _};

use tempfile::tempdir;

use git_ref_format::{lit, name, Namespaced, Qualified, RefString};
use it_helpers::{
    fixed::{TestPerson, TestProject},
    testnet::{self, RunningTestPeer},
    working_copy::{WorkingCopy, WorkingRemote as Remote},
};
use librad::git::{
    identities::{self, local, project::heads},
    storage::ReadOnlyStorage,
    tracking,
    types::{Namespace, Reference},
    Urn,
};
use link_identities::payload;
use test_helpers::logging;

fn config() -> testnet::Config {
    testnet::Config {
        num_peers: nonzero!(2usize),
        min_connected: 2,
        bootstrap: testnet::Bootstrap::from_env(),
    }
}

/// This test checks that the logic of `librad::git::identities::project::heads`
/// is correct. To do this we need to set up various scenarios where the
/// delegates of a project agree or disagree on the default branch of a project.
#[test]
fn default_branch_head() {
    logging::init();

    let net = testnet::run(config()).unwrap();
    net.enter(async {
        // Setup  a testnet with two peers and create a `Person` on each peer
        let peer1 = net.peers().index(0);
        let peer2 = net.peers().index(1);

        let id1 = peer1
            .using_storage::<_, anyhow::Result<TestPerson>>(|s| {
                let person = TestPerson::create(s)?;
                let local = local::load(s, person.owner.urn()).unwrap();
                s.config()?.set_user(local)?;
                Ok(person)
            })
            .await
            .unwrap()
            .unwrap();

        let id2 = peer2
            .using_storage::<_, anyhow::Result<TestPerson>>(|s| {
                let person = TestPerson::create(s)?;
                let local = local::load(s, person.owner.urn()).unwrap();
                s.config()?.set_user(local)?;
                Ok(person)
            })
            .await
            .unwrap()
            .unwrap();

        id2.pull(peer2, peer1).await.unwrap();
        id1.pull(peer1, peer2).await.unwrap();

        // Create a project on peer1 with both `Person`s as delegates
        let proj = peer1
            .using_storage({
                let owner = id1.owner.clone();
                move |s| {
                    TestProject::from_project_payload(
                        s,
                        owner,
                        payload::Project {
                            name: "venus".into(),
                            description: None,
                            default_branch: Some(name::MASTER.to_string().into()),
                        },
                    )
                }
            })
            .await
            .unwrap()
            .unwrap();

        // Track peer2 on peer1
        peer1
            .using_storage::<_, anyhow::Result<()>>({
                let urn = proj.project.urn();
                let peer2_id = peer2.peer_id();
                move |s| {
                    tracking::track(
                        s,
                        &urn,
                        Some(peer2_id),
                        tracking::Config::default(),
                        tracking::policy::Track::Any,
                    )??;
                    Ok(())
                }
            })
            .await
            .unwrap()
            .unwrap();

        proj.pull(peer1, peer2).await.unwrap();

        // Add peer2
        peer1
            .using_storage({
                let urn = proj.project.urn();
                let owner1 = id1.owner.clone();
                let owner2 = id2.owner.clone();
                move |storage| -> Result<(), anyhow::Error> {
                    identities::project::update(
                        storage,
                        &urn,
                        None,
                        None,
                        librad::identities::delegation::Indirect::try_from_iter(
                            vec![either::Either::Right(owner1), either::Either::Right(owner2)]
                                .into_iter(),
                        )
                        .unwrap(),
                    )?;
                    identities::project::verify(storage, &urn)?;
                    Ok(())
                }
            })
            .await
            .unwrap()
            .unwrap();

        proj.pull(peer1, peer2).await.unwrap();

        // Sign the project document using peer2
        peer2
            .using_storage({
                let urn = proj.project.urn();
                let peer_id = peer1.peer_id();
                let rad =
                    Urn::try_from(Reference::rad_id(Namespace::from(&urn)).with_remote(peer_id))
                        .unwrap();
                move |storage| -> Result<Option<identities::VerifiedProject>, anyhow::Error> {
                    let project = identities::project::get(&storage, &rad)?.unwrap();
                    identities::project::update(
                        storage,
                        &urn,
                        None,
                        None,
                        project.delegations().clone(),
                    )?;
                    identities::project::merge(storage, &urn, peer_id)?;
                    Ok(identities::project::verify(storage, &urn)?)
                }
            })
            .await
            .unwrap()
            .unwrap();

        proj.pull(peer2, peer1).await.unwrap();

        // Merge the signed update into peer1
        peer1
            .using_storage({
                let urn = proj.project.urn();
                let peer_id = peer2.peer_id();
                move |storage| -> Result<Option<identities::VerifiedProject>, anyhow::Error> {
                    identities::project::merge(storage, &urn, peer_id)?;
                    Ok(identities::project::verify(storage, &urn)?)
                }
            })
            .await
            .unwrap()
            .unwrap();

        id2.pull(peer2, peer1).await.unwrap();

        // Okay, now we have a running testnet with two Peers, each of which has a
        // `Person` who is a delegate on the `TestProject`

        // Create a commit in peer 1 and pull to peer2, then pull those changes into
        // peer2, create a new commit on top of the original commit and pull
        // that back to peer1. Then in peer1 pull the commit, fast forward, and
        // push.
        let tmp = tempdir().unwrap();
        let tip = {
            let mut working_copy1 =
                WorkingCopy::new(&proj, tmp.path().join("peer1"), peer1).unwrap();
            let mut working_copy2 =
                WorkingCopy::new(&proj, tmp.path().join("peer2"), peer2).unwrap();

            let mastor = Qualified::from(lit::refs_heads(name::MASTER));
            working_copy1
                .commit("peer 1 initial", mastor.clone())
                .unwrap();
            working_copy1.push().unwrap();
            proj.pull(peer1, peer2).await.unwrap();

            working_copy2.fetch(Remote::Peer(peer1.peer_id())).unwrap();
            working_copy2
                .create_remote_tracking_branch(Remote::Peer(peer1.peer_id()), name::MASTER)
                .unwrap();
            let tip = working_copy2
                .commit("peer 2 initial", mastor.clone())
                .unwrap();
            working_copy2.push().unwrap();
            proj.pull(peer2, peer1).await.unwrap();

            working_copy1.fetch(Remote::Peer(peer2.peer_id())).unwrap();
            working_copy1
                .fast_forward_to(Remote::Peer(peer2.peer_id()), name::MASTER)
                .unwrap();
            working_copy1.push().unwrap();
            tip
        };

        let default_branch = branch_head(peer1, &proj).await.unwrap();
        // The two peers hsould have the same view of the default branch
        assert_eq!(
            default_branch,
            identities::project::heads::DefaultBranchHead::Head {
                target: tip,
                branch: name::MASTER.to_owned(),
            }
        );

        // now update peer1 and push to peer 1s monorepo, we should get the tip of peer1
        // as the head (because peer2 can be fast forwarded)
        let tmp = tempdir().unwrap();
        let tip = {
            let mut working_copy1 =
                WorkingCopy::new(&proj, tmp.path().join("peer1"), peer1).unwrap();
            working_copy1
                .create_remote_tracking_branch(Remote::Rad, name::MASTER)
                .unwrap();

            let mastor = Qualified::from(lit::refs_heads(name::MASTER));
            let tip = working_copy1.commit("peer 1 fork", mastor.clone()).unwrap();
            working_copy1.push().unwrap();

            tip
        };

        let default_branch_peer1 = branch_head(peer1, &proj).await.unwrap();
        assert_eq!(
            default_branch_peer1,
            identities::project::heads::DefaultBranchHead::Head {
                target: tip,
                branch: name::MASTER.to_owned(),
            }
        );

        // now create an alternate commit on peer2 and sync with peer1, on peer1 we
        // should get a fork
        let tmp = tempdir().unwrap();
        let forked_tip = {
            let mut working_copy2 =
                WorkingCopy::new(&proj, tmp.path().join("peer2"), peer2).unwrap();
            working_copy2
                .create_remote_tracking_branch(Remote::Rad, name::MASTER)
                .unwrap();

            let mastor = Qualified::from(lit::refs_heads(name::MASTER));
            let forked_tip = working_copy2.commit("peer 2 fork", mastor.clone()).unwrap();
            working_copy2.push().unwrap();

            forked_tip
        };

        proj.pull(peer2, peer1).await.unwrap();

        let default_branch_peer1 = branch_head(peer1, &proj).await.unwrap();
        assert_eq!(
            default_branch_peer1,
            identities::project::heads::DefaultBranchHead::Forked(
                vec![
                    identities::project::heads::Fork {
                        ancestor_peers: std::collections::BTreeSet::new(),
                        tip_peers: std::iter::once(peer1.peer_id()).collect(),
                        tip,
                    },
                    identities::project::heads::Fork {
                        ancestor_peers: std::collections::BTreeSet::new(),
                        tip_peers: std::iter::once(peer2.peer_id()).collect(),
                        tip: forked_tip,
                    }
                ]
                .into_iter()
                .collect()
            )
        );

        // now update peer1 to match peer2
        let tmp = tempdir().unwrap();
        let fixed_tip = {
            let mut working_copy1 =
                WorkingCopy::new(&proj, tmp.path().join("peer1"), peer1).unwrap();
            working_copy1.fetch(Remote::Peer(peer2.peer_id())).unwrap();
            working_copy1
                .create_remote_tracking_branch(Remote::Peer(peer2.peer_id()), name::MASTER)
                .unwrap();

            working_copy1.fetch(Remote::Peer(peer2.peer_id())).unwrap();
            let tip = working_copy1
                .merge_remote(peer2.peer_id(), name::MASTER)
                .unwrap();
            working_copy1.push().unwrap();
            tip
        };

        let default_branch_peer1 = branch_head(peer1, &proj).await.unwrap();
        assert_eq!(
            default_branch_peer1,
            identities::project::heads::DefaultBranchHead::Head {
                target: fixed_tip,
                branch: name::MASTER.to_owned(),
            }
        );

        // now set the head in the monorepo and check that the HEAD reference exists
        let updated_tip = peer1
            .using_storage::<_, anyhow::Result<_>>({
                let urn = proj.project.urn();
                move |s| {
                    let vp = identities::project::verify(s, &urn)?.ok_or_else(|| {
                        anyhow::anyhow!("failed to get project for default branch")
                    })?;
                    identities::project::heads::set_default_head(s, vp).map_err(anyhow::Error::from)
                }
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated_tip, fixed_tip);

        let head_ref = RefString::try_from(format!(
            "refs/namespaces/{}/refs/HEAD",
            proj.project.urn().encode_id()
        ))
        .unwrap();
        let master_ref = Namespaced::from(lit::refs_namespaces(
            &proj.project.urn(),
            Qualified::from(lit::refs_heads(name::MASTER)),
        ));
        let (master_oid, head_oid) = peer1
            .using_storage::<_, anyhow::Result<_>>(move |s| {
                let master_oid = s
                    .reference(&master_ref.into_qualified().into_refstring())?
                    .ok_or_else(|| anyhow::anyhow!("master ref not found"))?
                    .peel_to_commit()?
                    .id();
                let head_oid = s
                    .reference(&head_ref)?
                    .ok_or_else(|| anyhow::anyhow!("head ref not found"))?
                    .peel_to_commit()?
                    .id();
                Ok((master_oid, head_oid))
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(master_oid, updated_tip);
        assert_eq!(head_oid, updated_tip);
    });
}

async fn branch_head(
    peer: &RunningTestPeer,
    proj: &TestProject,
) -> anyhow::Result<heads::DefaultBranchHead> {
    peer.using_storage::<_, anyhow::Result<_>>({
        let urn = proj.project.urn();
        move |s| {
            let vp = identities::project::verify(s, &urn)?
                .ok_or_else(|| anyhow::anyhow!("failed to get project for default branch"))?;
            heads::default_branch_head(s, vp).map_err(anyhow::Error::from)
        }
    })
    .await?
}
