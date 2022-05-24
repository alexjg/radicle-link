// Copyright © 2022 The Radicle Link Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use proptest::prelude::*;

use link_hooks::Data;
use link_identities_test::gen::urn::{gen_oid, gen_urn};
use radicle_git_ext as ext;

pub fn gen_data() -> impl Strategy<Value = Data<ext::Oid>> {
    gen_oid(git2::ObjectType::Commit).prop_flat_map(move |old| {
        gen_oid(git2::ObjectType::Commit)
            .prop_flat_map(move |new| gen_urn().prop_map(move |urn| Data { urn, old, new }))
    })
}
