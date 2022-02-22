// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::fmt;

use librad::{
    git::{
        identities::{relations, Doc, Identity, SomeIdentity, VerifiedIdentity},
        refs::Refs,
        Urn,
    },
    identities::payload::{Payload, PersonPayload, SomePayload},
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(bound(
    serialize = "T: serde::Serialize",
    deserialize = "T: serde::de::DeserializeOwned"
))]
pub struct Display<T> {
    urn: Urn,
    payload: T,
}

impl<T, D> From<Identity<Doc<Payload<T>, D>>> for Display<Payload<T>>
where
    T: Clone,
{
    fn from(i: Identity<Doc<Payload<T>, D>>) -> Self {
        (&i).into()
    }
}

impl<T, D> From<&Identity<Doc<Payload<T>, D>>> for Display<Payload<T>>
where
    T: Clone,
{
    fn from(i: &Identity<Doc<Payload<T>, D>>) -> Self {
        Self {
            urn: i.urn(),
            payload: i.payload().clone(),
        }
    }
}

impl<T, D> From<VerifiedIdentity<Doc<Payload<T>, D>>> for Display<Payload<T>>
where
    T: Clone,
{
    fn from(i: VerifiedIdentity<Doc<Payload<T>, D>>) -> Self {
        i.into_inner().into()
    }
}

impl From<SomeIdentity> for Display<SomePayload> {
    fn from(i: SomeIdentity) -> Self {
        Self {
            urn: i.urn(),
            payload: i.payload(),
        }
    }
}

impl<T> fmt::Display for Display<T>
where
    T: fmt::Debug + serde::Serialize,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match serde_json::to_string_pretty(&self.payload) {
            Ok(payload) => write!(f, "urn={}, payload={}", self.urn, payload),
            Err(_) => write!(f, "urn={}, payload={:?}", self.urn, self.payload),
        }
    }
}

/// A specialised display for [`relations::Persona`].
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Persona {
    urn: Urn,
    payload: PersonPayload,
    delegate: bool,
    refs: Option<Refs>,
}

impl From<relations::Persona> for Persona {
    fn from(persona: relations::Persona) -> Self {
        let person = persona.person();
        Self {
            urn: person.urn(),
            payload: person.payload().clone(),
            delegate: persona.delegate(),
            refs: persona.refs().cloned(),
        }
    }
}
