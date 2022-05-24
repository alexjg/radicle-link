// Copyright © 2022 The Radicle Link Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod data;
pub use data::Data;

pub mod track;
pub use track::Track;

pub enum Event {
    Track,
    Data,
}

pub trait IsZero {
    fn is_zero(&self) -> bool;
}

#[cfg(feature = "git")]
mod git {
    use git2::Oid;
    use radicle_git_ext as ext;

    use super::IsZero;

    impl IsZero for Oid {
        fn is_zero(&self) -> bool {
            self == &Oid::zero()
        }
    }

    impl IsZero for ext::Oid {
        fn is_zero(&self) -> bool {
            git2::Oid::from(*self).is_zero()
        }
    }
}

pub trait Hook {
    type Arg;
    type Error;

    fn run<I>(&self, arg: I) -> Result<(), Self::Error>
    where
        I: Iterator<Item = Self::Arg>;
}

// TODO(finto): Hook execution with arguments - end of process 0x04
// TODO(finto): Test the API surface of Data and Track
