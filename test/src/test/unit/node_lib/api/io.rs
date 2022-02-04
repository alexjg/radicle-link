// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use proptest::{array::uniform3, prelude::*};

use node_lib::api::{io, io::Transport as _, messages};

use crate::librad::identities::urn::{gen_oid, gen_urn};

fn user_agent() -> impl Strategy<Value = messages::UserAgent> {
    any::<String>().prop_map(|s| s.into())
}

fn request_id() -> impl Strategy<Value = messages::RequestId> {
    any::<Vec<u8>>().prop_map(|s| s.into())
}

fn request_mode() -> impl Strategy<Value = messages::RequestMode> {
    prop_oneof! {
        Just(messages::RequestMode::ReportProgress),
        Just(messages::RequestMode::FireAndForget),
    }
}

prop_compose! {
    fn request_payload()
        (rev in gen_oid(git2::ObjectType::Commit),
         urn in gen_urn())
        -> messages::RequestPayload {
        messages::RequestPayload::Announce{
            rev: rev.into(),
            urn,
        }
    }
}

prop_compose! {
    fn request()
        (user_agent in user_agent(),
         mode in request_mode(),
         payload in request_payload())
        -> messages::Request {
        messages::Request{
            user_agent,
            mode,
            payload,
        }

    }
}

fn response_payload() -> impl Strategy<Value = messages::ResponsePayload> {
    prop_oneof! {
        any::<String>().prop_map(messages::ResponsePayload::Progress),
        any::<String>().prop_map(messages::ResponsePayload::Error),
        Just(messages::ResponsePayload::Success),
    }
}

prop_compose! {
    fn response()
        (payload in response_payload(),
         id in request_id())
        -> messages::Response {
        messages::Response{
            payload,
            request_id: id,
        }
    }
}

proptest! {
    #[test]
    fn test_request_round_trip(requests in uniform3(request())) {
        with_async_transport(
            |mut left, mut right| async move  {
                let mut result = Vec::new();
                for request in &requests {
                    left.send_request(request.clone()).await.unwrap();
                }
                while result.len() < 3 {
                    let message = right.recv_request().await.unwrap();
                    result.push(message.unwrap());
                }
                drop(left);
                assert!(right.recv_request().await.unwrap().is_none());
                assert_eq!(requests.to_vec(), result);
            }
        )
    }

    #[test]
    fn test_response_round_trip(responses in uniform3(response())) {
        with_async_transport(|mut left, mut right| async move{
            let mut result = Vec::new();
            for response in &responses {
                left.send_response(response.clone()).await.unwrap();
            }
            while result.len() < 3 {
                let message = right.recv_response().await.unwrap();
                result.push(message.unwrap());
            }
            drop(left);
            assert!(right.recv_response().await.unwrap().is_none());
            assert_eq!(responses.to_vec(), result);
        })
    }
}

fn with_async_transport<
    F: FnOnce(io::SocketTransport, io::SocketTransport) -> FU,
    FU: futures::Future<Output = ()>,
>(
    f: F,
) {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let (left, right) = tokio::net::UnixStream::pair().unwrap();
            f(left.into(), right.into()).await
        })
}
