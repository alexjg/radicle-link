use std::{fmt, path::PathBuf, sync::Arc};

use tracing::instrument;

use librad::git::{
    refs::{self, Refs},
    storage,
    Urn,
};
use link_async::Spawner;
use linkd_lib::api::client::Reply;

#[derive(Clone)]
pub(crate) struct Hooks {
    spawner: Arc<Spawner>,
    pool: Arc<storage::Pool<storage::Storage>>,
    rpc_socket_path: Option<PathBuf>,
    post_receive: PostReceive,
}

impl fmt::Debug for Hooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hooks")
            .field("post_receive", &self.post_receive)
            .finish()
    }
}

pub(crate) struct Progress(String);

impl fmt::Display for Progress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Progress {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for Progress {
    fn from(s: String) -> Self {
        Self(s)
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error<E: std::error::Error + Send + 'static> {
    #[error("error notifying client of progress: {0}")]
    Progress(E),
    #[error("could not open storage: {0}")]
    OpenStorage(Box<dyn std::error::Error + Send + 'static>),
    #[error("error updating refs: {0}")]
    UpdateRefs(Box<dyn std::error::Error + Send + 'static>),
    #[error("failed to connect to linkd node: {0}")]
    LinkdConnect(Box<dyn std::error::Error + Send + 'static>),
    #[error("linkd rpc transport failed: {0}")]
    LinkdTransport(Box<dyn std::error::Error + Send + 'static>),
    #[error("the linkd node reported an error: {0}")]
    Linkd(String),
}

pub(crate) trait ProgressReporter {
    type Error;
    fn report(&mut self, progress: Progress)
        -> futures::future::BoxFuture<Result<(), Self::Error>>;
}

impl Hooks {
    pub(crate) fn update_sigrefs(
        spawner: Arc<Spawner>,
        pool: Arc<storage::Pool<storage::Storage>>,
    ) -> Hooks {
        Self {
            spawner,
            pool,
            post_receive: PostReceive { announce: false },
            rpc_socket_path: None,
        }
    }

    pub(crate) fn announce(
        spawner: Arc<Spawner>,
        pool: Arc<storage::Pool<storage::Storage>>,
        rpc_socket_path: Option<PathBuf>,
    ) -> Hooks {
        Self {
            spawner,
            pool,
            post_receive: PostReceive { announce: true },
            rpc_socket_path,
        }
    }

    #[instrument(skip(self, reporter))]
    pub(crate) async fn post_receive<
        E: std::error::Error + Send + 'static,
        P: ProgressReporter<Error = E>,
    >(
        self,
        mut reporter: P,
        urn: Urn,
    ) -> Result<(), Error<E>> {
        // Update `rad/signed_refs`
        reporter
            .report("updating signed refs".into())
            .await
            .map_err(Error::Progress)?;
        let update_result = {
            let storage = self
                .pool
                .get()
                .await
                .map_err(|e| Error::OpenStorage(Box::new(e)))?;
            let urn = urn.clone();
            self.spawner
                .blocking::<_, Result<_, refs::stored::Error>>(move || {
                    Refs::update(storage.as_ref(), &urn)
                })
                .await
                .map_err(|e| {
                    tracing::error!(err=?e, "error updating signed refs");
                    Error::UpdateRefs(Box::new(e))
                })
        }?;
        let at = match update_result {
            refs::Updated::Updated { at, .. } => at,
            refs::Updated::Unchanged { at, .. } => at,
            refs::Updated::ConcurrentlyModified => {
                tracing::warn!("attempted concurrent updates of signed refs");
                reporter
                    .report(
                        "sigrefs race whilst updating signed refs, you may need to retry".into(),
                    )
                    .await
                    .map_err(Error::Progress)?;
                return Ok(());
            },
        };
        reporter
            .report("signed refs updated".into())
            .await
            .map_err(Error::Progress)?;

        if self.post_receive.announce {
            tracing::info!("running post receive announcement hook");
            if let Some(rpc_socket_path) = self.rpc_socket_path {
                reporter
                    .report("announcing new refs".into())
                    .await
                    .map_err(Error::Progress)?;
                tracing::trace!(?rpc_socket_path, "attempting to send announcement");
                let conn =
                    linkd_lib::api::client::Connection::connect("linkd-git", rpc_socket_path)
                        .await
                        .map_err(|e| Error::LinkdConnect(Box::new(e)))?;
                let cmd = linkd_lib::api::client::Command::announce(urn.clone(), at.into());
                let mut replies = cmd
                    .execute_with_reply(conn)
                    .await
                    .map_err(|e| Error::LinkdTransport(Box::new(e)))?;
                loop {
                    match replies.next().await {
                        Ok(Reply::Progress {
                            replies: next_replies,
                            msg,
                        }) => {
                            tracing::trace!(?msg, "got progress messaage from linkd node");
                            reporter.report(msg.into()).await.map_err(Error::Progress)?;
                            replies = next_replies;
                        },
                        Ok(Reply::Success { .. }) => {
                            tracing::trace!("got success from linkd node");
                            reporter
                                .report("succesful announcement".into())
                                .await
                                .map_err(Error::Progress)?;
                            break;
                        },
                        Ok(Reply::Error { msg, .. }) => {
                            tracing::error!(?msg, "got error from liinkd node");
                            return Err(Error::Linkd(msg));
                        },
                        Err((_, e)) => {
                            tracing::error!(err=?e, "error communicating with linkd node");
                            return Err(Error::LinkdTransport(Box::new(e)));
                        },
                    }
                }
            } else {
                tracing::warn!("no link-rpc-socket to announce to");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct PostReceive {
    announce: bool,
}
