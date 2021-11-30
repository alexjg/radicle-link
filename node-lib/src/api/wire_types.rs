// Copyright © 2021 The Radicle Foundation <hello@radicle.foundation>
// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

pub mod events {
    use librad::{git::Urn, net::protocol::gossip};

    #[derive(minicbor::Decode, minicbor::Encode)]
    pub struct Envelope {
        #[n(1)]
        pub message: Option<Message>,
    }

    #[derive(minicbor::Decode, minicbor::Encode)]
    pub enum Message {
        #[n(0)]
        PostReceive(#[n(0)] PostReceive),
    }

    #[derive(minicbor::Decode, minicbor::Encode)]
    pub struct PostReceive {
        #[n(0)]
        pub urn: Urn,
        #[n(1)]
        pub rev: radicle_git_ext::Oid,
    }

    impl From<PostReceive> for gossip::Payload {
        fn from(pr: PostReceive) -> Self {
            gossip::Payload {
                urn: pr.urn,
                origin: None,
                rev: Some(pr.rev.into()),
            }
        }
    }
}
