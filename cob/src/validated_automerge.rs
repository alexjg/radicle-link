// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use super::{History, Schema};

use std::convert::TryFrom;

pub mod error {
    use super::super::schema::error::Parse as SchemaParseError;
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum LoadError {
        #[error(transparent)]
        AutomergeBackend(#[from] automerge::BackendError),
        #[error(transparent)]
        AutomergeFrontend(#[from] automerge::FrontendError),
        #[error(transparent)]
        InvalidPatch(#[from] automerge::InvalidPatch),
        #[error(transparent)]
        SchemaParse(#[from] SchemaParseError),
    }

    #[derive(Debug, Error)]
    pub enum ProposalError {
        #[error("invalid change: {0}")]
        InvalidChange(Box<dyn std::error::Error>),
        #[error("invalidates schema: {0}")]
        InvalidatesSchema(Box<dyn std::error::Error>),
    }
}

/// A history which is valid with respect to a schema and allows fallibly
/// proposing a new change
///
/// The main purpose of this is to cache the backend and frontend for use when
/// the change does not invalidate the schema (presumably the common case). This
/// is necessary because loading a schema invalidating change requires throwing
/// away the backend and reloading it, which is very wasteful for the happy
/// path.
pub(crate) struct ValidatedAutomerge {
    backend: automerge::Backend,
    frontend: automerge::Frontend,
    schema: Schema,
    valid_history: Vec<u8>,
}

impl ValidatedAutomerge {
    pub(crate) fn new(schema: Schema) -> ValidatedAutomerge {
        ValidatedAutomerge {
            backend: automerge::Backend::new(),
            frontend: automerge::Frontend::new(),
            valid_history: Vec::new(),
            schema,
        }
    }

    pub(crate) fn new_with_history(
        schema: Schema,
        history: Vec<u8>,
    ) -> Result<ValidatedAutomerge, error::LoadError> {
        let backend = automerge::Backend::load(history.clone())?;
        let mut frontend = automerge::Frontend::new();
        frontend.apply_patch(backend.get_patch().unwrap()).unwrap();
        Ok(ValidatedAutomerge {
            backend,
            frontend,
            valid_history: history,
            schema,
        })
    }

    pub(crate) fn propose_change(
        &mut self,
        change_bytes: &[u8],
    ) -> Result<(), error::ProposalError> {
        let change = automerge::Change::try_from(change_bytes)
            .map_err(|e| error::ProposalError::InvalidChange(Box::new(e)))?;
        let old_backend = self.backend.clone();
        let patch = self
            .backend
            .apply_changes(vec![change])
            .map_err(|e| error::ProposalError::InvalidChange(Box::new(e)))?;
        self.frontend.apply_patch(patch).unwrap();
        let value = self.frontend.get_value(&automerge::Path::root()).unwrap();
        let validation_error = self.schema.validate(&value.to_json()).err();
        match validation_error {
            None => {
                self.valid_history.extend(change_bytes);
                Ok(())
            },
            Some(e) => {
                tracing::debug!(invalid_json=?value.to_json().to_string(), "change invalidated schema");
                self.reset(old_backend);
                Err(error::ProposalError::InvalidatesSchema(Box::new(e)))
            },
        }
    }

    fn reset(&mut self, old_backend: automerge::Backend) {
        self.backend = old_backend;
        let mut old_frontend = automerge::Frontend::new();
        let patch = self.backend.get_patch().unwrap();
        old_frontend.apply_patch(patch).unwrap();
        self.frontend = old_frontend;
    }

    pub(crate) fn valid_history(&self) -> History {
        History::Automerge(self.valid_history.clone())
    }

    pub(crate) fn compressed_valid_history(&self) -> History {
        History::Automerge(self.backend.save().unwrap())
    }

    pub(crate) fn raw(&self) -> &[u8] {
        &self.valid_history
    }
}
