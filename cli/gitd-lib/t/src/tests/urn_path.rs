use proptest::prelude::*;
use link_crypto_test::gen::gen_peer_id;
use link_identities_test::gen::urn::gen_urn;
use gitd_lib::ssh_service::UrnPath;
use std::str::FromStr;

proptest!{
    #[test]
    fn parse_urnpath(urn in gen_urn(), peer in proptest::option::of(gen_peer_id())) {
        let urn = urn.with_path(None);
        let path = if let Some(peer) = peer {
            format!("{}/{}.git", urn, peer)
        } else {
            format!("{}.git", urn)
        };
        let parsed = UrnPath::from_str(&path).unwrap();
        assert_eq!(parsed, UrnPath::new(urn, peer));
    }
}

