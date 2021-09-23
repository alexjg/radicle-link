// Copyright © 2019-2020 The Radicle Foundation <hello@radicle.foundation>
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

mod author_commit {
    super::oid_trailer! {AuthorCommitTrailer, "X-Rad-Author"}
}
mod containing_identity {
    super::oid_trailer! {ContainingIdentityCommitTrailer, "X-Rad-Containing-Identity"}
}
mod schema_commit {
    super::oid_trailer! {SchemaCommitTrailer, "X-Rad-Schema"}
}

pub mod error {
    pub use super::author_commit::Error as InvalidAuthorTrailer;

    pub use super::schema_commit::Error as InvalidSchemaTrailer;

    pub use super::containing_identity::Error as InvalidContainingIdentityTrailer;
}

pub use author_commit::AuthorCommitTrailer;
pub use containing_identity::ContainingIdentityCommitTrailer;
pub use schema_commit::SchemaCommitTrailer;

/// A macro for generating boilerplate From and TryFrom impls for trailers which
/// have git object IDs as their values
#[macro_export]
macro_rules! oid_trailer {
    ($typename:ident, $trailer:literal) => {
        use git_trailers::{OwnedTrailer, Token, Trailer};
        use radicle_git_ext as ext;

        use std::convert::{TryFrom, TryInto};

        #[derive(Debug)]
        pub enum Error {
            NoTrailer,
            NoValue,
            InvalidOid,
        }

        // We can't use `derive(thiserror::Error)` as we need to concat strings with
        // $trailer and macros are not allowed in non-key-value attributes
        impl std::fmt::Display for Error {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    Self::NoTrailer => write!(f, concat!("no ", $trailer)),
                    Self::NoValue => write!(f, concat!("no value for ", $trailer, " trailer")),
                    Self::InvalidOid => write!(f, "invalid git OID"),
                }
            }
        }

        impl std::error::Error for Error {}

        pub struct $typename(git2::Oid);

        impl $typename {
            pub fn oid(&self) -> git2::Oid {
                self.0
            }

            pub fn from_trailers<'b, A, I>(trailers: I) -> Result<$typename, Error>
            where
                A: Into<Trailer<'b>>,
                I: IntoIterator<Item = A>,
            {
                trailers
                    .into_iter()
                    .find_map(|trailer| {
                        let trailer = trailer.into();
                        if trailer.token == Token::try_from($trailer).unwrap() {
                            Some($typename::try_from(&trailer))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(Err(Error::NoTrailer))
            }
        }

        impl From<git2::Oid> for $typename {
            fn from(oid: git2::Oid) -> Self {
                $typename(oid)
            }
        }

        impl From<$typename> for Trailer<'_> {
            fn from(containing: $typename) -> Self {
                Trailer {
                    token: Token::try_from($trailer).unwrap(),
                    values: vec![containing.0.to_string().into()],
                }
            }
        }

        impl TryFrom<Vec<Trailer<'_>>> for $typename {
            type Error = Error;

            fn try_from(trailers: Vec<Trailer<'_>>) -> Result<Self, Self::Error> {
                $typename::from_trailers(trailers)
            }
        }

        impl TryFrom<&Trailer<'_>> for $typename {
            type Error = Error;

            fn try_from(Trailer { values, .. }: &Trailer<'_>) -> Result<Self, Self::Error> {
                let val = values.first().ok_or(Error::NoValue)?;
                let oid = git2::Oid::from_str(val).map_err(|_| Error::InvalidOid)?;
                Ok($typename(oid))
            }
        }

        impl TryFrom<&OwnedTrailer> for $typename {
            type Error = Error;

            fn try_from(trailer: &OwnedTrailer) -> Result<Self, Self::Error> {
                (&Trailer::from(trailer)).try_into()
            }
        }

        impl TryFrom<Vec<OwnedTrailer>> for $typename {
            type Error = Error;

            fn try_from(trailers: Vec<OwnedTrailer>) -> Result<Self, Self::Error> {
                let trailer_refs = trailers.iter().map(Trailer::from);
                $typename::from_trailers(trailer_refs)
            }
        }

        impl TryFrom<&[OwnedTrailer]> for $typename {
            type Error = Error;

            fn try_from(trailers: &[OwnedTrailer]) -> Result<Self, Self::Error> {
                let trailer_refs = trailers.iter().map(Trailer::from);
                $typename::from_trailers(trailer_refs)
            }
        }

        impl From<ext::Oid> for $typename {
            fn from(oid: ext::Oid) -> Self {
                $typename(oid.into())
            }
        }
    };
}
pub(crate) use oid_trailer;
