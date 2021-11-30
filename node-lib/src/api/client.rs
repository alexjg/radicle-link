// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use radicle_git_ext as git_ext;

use std::{default::Default, os::unix::net::UnixStream};

use librad::git::Urn;

use super::wire_types::rpc;

pub trait RpcClient {
    type Error: std::error::Error;

    fn announce(&mut self, urn: Urn, revision: git_ext::Oid) -> Result<(), Self::Error>;
}

pub struct SocketRpcClient {
    socket: UnixStream,
    user_agent: String,
}

impl SocketRpcClient {
    pub fn connect<C: ToString, P: AsRef<std::path::Path>>(
        user_agent: C,
        socket_path: P,
    ) -> Result<SocketRpcClient, std::io::Error> {
        let stream = UnixStream::connect(socket_path)?;
        Ok(SocketRpcClient {
            socket: stream,
            user_agent: user_agent.to_string(),
        })
    }

    fn headers(&self) -> rpc::RequestHeaders {
        rpc::RequestHeaders {
            user_agent: self.user_agent.as_str().into(),
            request_id: rpc::RequestId::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SocketRpcError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("the RPC endpoint returned an empty response")]
    NoResponse,
    #[error("RPC endpoint returned an error: {0}")]
    RpcError(String),
    #[error("unknown message in protocol")]
    UnknownMessage,
    #[error(transparent)]
    Minicbor(#[from] minicbor::decode::Error),
    #[error(transparent)]
    MinicborIo(#[from] minicbor_io::Error),
}

impl RpcClient for SocketRpcClient {
    type Error = SocketRpcError;

    fn announce(&mut self, urn: Urn, revision: git_ext::Oid) -> Result<(), Self::Error> {
        let message = rpc::RequestEnvelope {
            headers: self.headers(),
            payload: Some(rpc::RequestPayload::Announce { urn, rev: revision }),
        };
        let mut writer = minicbor_io::Writer::new(&self.socket);
        writer.write(message)?;
        let mut reader = minicbor_io::Reader::new(&self.socket);
        let envelope = reader.read::<'_, rpc::ResponseEnvelope>()?;
        if let Some(envelope) = envelope {
            match envelope.response {
                Some(rpc::ResponsePayload::AnnounceSuccess) => Ok(()),
                Some(rpc::ResponsePayload::Error { description, .. }) => {
                    Err(Self::Error::RpcError(description))
                },
                None => Err(Self::Error::UnknownMessage),
            }
        } else {
            Err(Self::Error::NoResponse)
        }
    }
}
