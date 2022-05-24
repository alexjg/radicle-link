// Copyright © 2022 The Radicle Link Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use proptest::prelude::*;

use test_helpers::roundtrip;

use crate::gen::{data::gen_data, track::gen_track};

// #[test]
// fn roundtrip() {
//     let track = link_hooks::Track {
//         urn: link_identities::urn::Urn {
//             id: "d238753c66687be890be6e434c132457422889a5"
//                 .parse::<radicle_git_ext::Oid>()
//                 .unwrap(),
//             path: Some(radicle_git_ext::RefLike::try_from("1mvwk").unwrap()),
//         },
//         peer: "hyb558i4w81syeoest7oabc8ia8cdn9piremk7uz9eqxwqj575fc8c"
//             .parse()
//             .map(Some)
//             .unwrap(),
//         old: "dcf5b16e76cce7425d0beaef62d79a7d10fce1f5"
//             .parse::<radicle_git_ext::Oid>()
//             .unwrap(),
//         new: "4d4f8376cd8d37a935b3d5dd0eb855c40b519341"
//             .parse::<radicle_git_ext::Oid>()
//             .unwrap(),
//     };
//     print!("WTF: {}\n", track);
//     roundtrip::str(track)
// }

proptest! {
    #[test]
    fn roundtrip_data(data in gen_data()) {
        roundtrip::str(data)
    }

    #[test]
    fn roundtrip_track(track in gen_track()) {
        print!("WTF: {}", track);
        roundtrip::str(track)
    }
}
