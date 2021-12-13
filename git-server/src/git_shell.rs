use std::{fmt::Debug, path::PathBuf, process::Stdio, sync::Arc};

use futures::FutureExt;
use git2::transport::Service as GitService;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use librad::{
    git::{
        identities,
        refs::{self, Refs},
        storage::{self, glob, ReadOnlyStorage as _},
        types::Namespace,
        Urn,
    },
    reflike,
};

use link_async::{Spawner, Task};
use node_lib::api::client::RpcClient;
use radicle_git_ext as ext;

use crate::service::Service;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Pool(#[from] storage::pool::PoolError),
    #[error("no such urn: {0}")]
    NoSuchUrn(Urn),
    #[error("sigrefs race")]
    SigrefsRace,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Git2(#[from] git2::Error),
    #[error(transparent)]
    Storage(#[from] librad::git::storage::Error),
    #[error(transparent)]
    Identities(#[from] identities::Error),
    #[error(transparent)]
    Stored(#[from] refs::stored::Error),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    #[error("error spawning git: {0}")]
    SpawnGit(std::io::Error),
}

pub struct RunningGitShell {
    pub(crate) send: tokio::sync::mpsc::Sender<Vec<u8>>,
    pub(crate) task: Task<(thrussh::ChannelId, Result<(), Error>)>,
}

impl RunningGitShell {
    #[tracing::instrument(level = "trace", skip(spawner, pool, handle))]
    pub(crate) fn start(
        spawner: Arc<Spawner>,
        pool: Arc<storage::Pool<storage::Storage>>,
        id: thrussh::ChannelId,
        handle: thrussh::server::Handle,
        service: Service,
        urn: Urn,
        rpc_socket_path: PathBuf,
    ) -> RunningGitShell {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let inner_spawner = spawner.clone();
        let task = spawner.spawn(async move {
            let result = handle_git(inner_spawner, rx, handle, id, service, urn, pool, rpc_socket_path).await;
            (id, result)
        });
        RunningGitShell { send: tx, task }
    }
}

#[tracing::instrument(level = "debug", skip(spawner, incoming, out, pool))]
pub(crate) async fn handle_git(
    spawner: Arc<Spawner>,
    mut incoming: tokio::sync::mpsc::Receiver<Vec<u8>>,
    mut out: thrussh::server::Handle,
    id: thrussh::ChannelId,
    service: Service,
    urn: Urn,
    pool: Arc<storage::Pool<storage::Storage>>,
    rpc_socket_path: PathBuf,
) -> Result<(), Error> {
    let mut git = {
        let storage = pool.get().await?;
        let urn = urn.clone();
        spawner
            .blocking::<_, Result<_, _>>(move || create_command(&storage, urn, service))
            .await?
    };

    let mut child = git
        .arg(".")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(Error::SpawnGit)?;

    let mut child_stdin = child.stdin.take().unwrap();
    let mut child_stdout = child.stdout.take().unwrap();

    let complete = child.wait().fuse();
    futures::pin_mut!(complete);

    let mut buffer = [0; 1000];
    loop {
        futures::select! {
            input = incoming.recv().fuse() => {
                tracing::trace!("got incoming");
                if let Some(bytes) = input {
                    child_stdin.write_all(&bytes[..]).await?;
                }
            },
            bytes_read = child_stdout.read(&mut buffer).fuse() => {
                let bytes_read = bytes_read?;
                tracing::trace!(num_bytes=bytes_read, "read some data");
                let data: Vec<u8> = buffer[0..bytes_read].into();
                out.data(id, data.into()).await.map_err(|_| anyhow::anyhow!("failed to send data to handle"))?;
            },
            _ = complete => {
                tracing::trace!("child finished");
                break;
            }
        };
    }

    loop {
        let n = child_stdout.read(&mut buffer).await?;
        if n <= 0 {
            break;
        }
        let data: Vec<u8> = buffer[0..n].into();
        out.data(id, data.into())
            .await
            .map_err(|_| anyhow::anyhow!("failed to send data to handle"))?;
    }

    match service.0 {
        GitService::ReceivePack => {
            // Update `rad/signed_refs`
            let update_result = {
                let storage = pool.get().await?;
                let urn = urn.clone();
                spawner
                    .blocking::<_, Result<_, refs::stored::Error>>(move || {
                        Refs::update(storage.as_ref(), &urn)
                    })
                    .await
            }?;
            if let refs::Updated::ConcurrentlyModified = update_result {
                return Err(Error::SigrefsRace);
            }

            if let refs::Updated::Updated{at, ..} = update_result {
                let mut client = node_lib::api::client::SocketRpcClient::connect("linkd-git", rpc_socket_path)?;
                spawner.blocking(move ||{
                    client.announce(urn, at.into())
                }).await.map_err(|e| anyhow::anyhow!("failed to announce: {}", e))?;
            }
        }

        _ => {}
    };

    out.exit_status_request(id, 0)
        .await
        .map_err(|_| anyhow::anyhow!("could not send exit status"))?;

    out.close(id).await.map_err(|_| anyhow::anyhow!("could not close channel"))?;

    Ok(())
}

fn create_command(
    storage: &storage::Storage,
    urn: Urn,
    service: Service,
) -> Result<tokio::process::Command, anyhow::Error> {
    guard_has_urn(storage, &urn)?;

    let mut git = tokio::process::Command::new("git");
    git.current_dir(&storage.path()).args(&[
        &format!("--namespace={}", Namespace::from(&urn)),
        "-c",
        "transfer.hiderefs=refs/",
        "-c",
        "transfer.hiderefs=!refs/heads",
        "-c",
        "transfer.hiderefs=!refs/tags",
    ]);

    match service.0 {
        GitService::UploadPack | GitService::UploadPackLs => {
            // Fetching remotes is ok, pushing is not
            visible_remotes(storage, &urn)?.for_each(|remote_ref| {
                git.arg("-c")
                    .arg(format!("uploadpack.hiderefs=!^{}", remote_ref));
            });
            git.args(&["upload-pack", "--strict", "--timeout=5"]);
        }

        GitService::ReceivePack | GitService::ReceivePackLs => {
            git.arg("receive-pack");
        }
    }

    if matches!(
        service.0,
        GitService::UploadPackLs | GitService::ReceivePackLs
    ) {
        git.arg("--advertise-refs");
    }
    Ok(git)
}

fn guard_has_urn<S>(storage: S, urn: &Urn) -> Result<(), Error>
where
    S: AsRef<librad::git::storage::ReadOnly>,
{
    let have = storage.as_ref().has_urn(&urn).map_err(Error::from)?;
    if !have {
        Err(Error::NoSuchUrn(urn.clone()))
    } else {
        Ok(())
    }
}

fn visible_remotes<S>(
    storage: S,
    urn: &Urn,
) -> Result<impl Iterator<Item = ext::RefLike>, anyhow::Error>
where
    S: AsRef<librad::git::storage::ReadOnly>,
{
    let remotes = storage
        .as_ref()
        .references_glob(visible_remotes_glob(&urn))?
        .filter_map(move |res| {
            res.map(|reference| {
                reference
                    .name()
                    .and_then(|name| ext::RefLike::try_from(name).ok())
            })
            .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(remotes.into_iter())
}

pub fn visible_remotes_glob(urn: &Urn) -> impl glob::Pattern + Debug {
    globset::Glob::new(&format!(
        "{}/*/{{heads,tags}}/*",
        reflike!("refs/namespaces")
            .join(Namespace::from(urn))
            .join(reflike!("refs/remotes"))
    ))
    .unwrap()
    .compile_matcher()
}
