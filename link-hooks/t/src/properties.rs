// Copyright © 2022 The Radicle Link Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use proptest::prelude::*;

use link_hooks::{Data, IsZero, Track};
use radicle_git_ext as ext;
use test_helpers::roundtrip;

use crate::gen::{data::gen_data, track::gen_track};

proptest! {
    #[test]
    fn roundtrip_data(data in gen_data()) {
        roundtrip::str(data)
    }

    #[test]
    fn roundtrip_track(track in gen_track()) {
        roundtrip::str(track)
    }

    #[test]
    fn track_updated(track in gen_track()) {
        prop_track_created(track.clone());
        prop_track_deleted(track.clone());
        prop_track_changed(track.clone());
    }

        #[test]
    fn data_updated(data in gen_data()) {
        prop_data_created(data.clone());
        prop_data_deleted(data.clone());
        prop_data_changed(data.clone());
    }
}

fn prop_track_created(track: Track<ext::Oid>) {
    if !track.new.is_zero() {
        let track = Track {
            old: git2::Oid::zero().into(),
            ..track
        };

        assert!(track.is_created());
    }
}

fn prop_track_deleted(track: Track<ext::Oid>) {
    if !track.old.is_zero() {
        let track = Track {
            new: git2::Oid::zero().into(),
            ..track
        };

        assert!(track.is_deleted());
    }
}

fn prop_track_changed(track: Track<ext::Oid>) {
    if !track.old.is_zero() && !track.new.is_zero() {
        assert!(track.is_changed());
    }
}

fn prop_data_created(data: Data<ext::Oid>) {
    if !data.new.is_zero() {
        let data = Data {
            old: git2::Oid::zero().into(),
            ..data
        };

        assert!(data.is_created());
    }
}

fn prop_data_deleted(data: Data<ext::Oid>) {
    if !data.old.is_zero() {
        let data = Data {
            new: git2::Oid::zero().into(),
            ..data
        };

        assert!(data.is_deleted());
    }
}

fn prop_data_changed(data: Data<ext::Oid>) {
    if !data.old.is_zero() && !data.new.is_zero() {
        assert!(data.is_changed());
    }
}
