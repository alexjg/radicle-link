// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

pub mod rpc {
    use librad::git::Urn;
    use rand::Rng;

    #[derive(minicbor::Decode, minicbor::Encode)]
    #[cbor(map)]
    pub struct RequestEnvelope {
        #[n(0)]
        pub headers: RequestHeaders,
        #[n(1)]
        pub payload: Option<RequestPayload>,
    }

    #[derive(minicbor::Decode, minicbor::Encode)]
    #[cbor(map)]
    pub enum RequestPayload {
        #[n(0)]
        #[cbor(map)]
        Announce {
            #[n(0)]
            urn: Urn,
            #[n(1)]
            rev: radicle_git_ext::Oid,
        },
    }

    #[derive(minicbor::Decode, minicbor::Encode)]
    #[cbor(map)]
    pub struct ResponseEnvelope {
        #[n(0)]
        pub response_headers: ResponseHeaders,
        #[n(1)]
        pub response: Option<ResponsePayload>,
    }

    #[derive(minicbor::Decode, minicbor::Encode)]
    pub enum ResponsePayload {
        #[n(0)]
        AnnounceSuccess,
        #[n(1)]
        #[cbor(map)]
        Error {
            #[n(0)]
            message: String,
            #[n(1)]
            kind: Option<ErrorKind>,
        },
    }

    #[derive(minicbor::Decode, minicbor::Encode)]
    #[cbor(index_only)]
    pub enum ErrorKind {
        /// Some error occurred
        #[n(0)]
        Internal,
    }

    #[derive(minicbor::Decode, minicbor::Encode)]
    #[cbor(map)]
    pub struct RequestHeaders {
        #[n(0)]
        pub user_agent: UserAgent,
        #[n(1)]
        pub request_id: RequestId,
    }

    #[derive(minicbor::Decode, minicbor::Encode)]
    #[cbor(transparent)]
    pub struct UserAgent(#[n(0)] String);

    impl From<&str> for UserAgent {
        fn from(s: &str) -> Self {
            Self(s.to_string())
        }
    }

    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Decode, minicbor::Encode)]
    #[cbor(transparent)]
    pub struct RequestId(#[n(0)] minicbor::bytes::ByteVec);

    impl Default for RequestId {
        fn default() -> Self {
            let mut rng = rand::thread_rng();
            let bytes: [u8; 16] = rng.gen();
            let bytevec: minicbor::bytes::ByteVec = bytes.to_vec().into();
            RequestId(bytevec)
        }
    }

    impl AsRef<[u8]> for RequestId {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    #[derive(minicbor::Decode, minicbor::Encode)]
    #[cbor(map)]
    pub struct ResponseHeaders {
        #[n(0)]
        request_id: RequestId,
    }

    impl From<RequestId> for ResponseHeaders {
        fn from(id: RequestId) -> Self {
            ResponseHeaders { request_id: id }
        }
    }
}
