use std::{
    collections::{HashMap, HashSet},
    panic,
    path::PathBuf,
    sync::Arc,
};

use futures::{
    select,
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};
use librad::git::{storage::pool::Pool, storage::Storage, Urn};
use link_async::{Spawner, Task};

use crate::{git_shell, service::Service};

enum Incoming {
    ExecGit {
        service: Service,
        urn: Urn,
        channel: thrussh::ChannelId,
        handle: thrussh::server::Handle,
    },
    DataReceived {
        channel: thrussh::ChannelId,
        data: Vec<u8>,
    },
    Stop,
}

pub(crate) struct Processes {
    spawner: Arc<Spawner>,
    pool: Arc<Pool<Storage>>,
    incoming: tokio::sync::mpsc::Receiver<Incoming>,
    process_sends: HashMap<thrussh::ChannelId, tokio::sync::mpsc::Sender<Vec<u8>>>,
    running_processes: FuturesUnordered<Task<(thrussh::ChannelId, Result<(), git_shell::Error>)>>,
    dead_sends: HashSet<thrussh::ChannelId>,
    rpc_socket_path: PathBuf,
}

#[derive(Clone)]
pub(crate) struct ProcessesHandle {
    send: tokio::sync::mpsc::Sender<Incoming>,
}

impl ProcessesHandle {
    pub(crate) async fn send(
        &self,
        id: thrussh::ChannelId,
        data: Vec<u8>,
    ) -> Result<(), anyhow::Error> {
        self.send
            .send(Incoming::DataReceived { channel: id, data })
            .await
            .map_err(|_| anyhow::anyhow!("failed to send data"))
    }

    pub(crate) async fn exec_git(
        &self,
        channel: thrussh::ChannelId,
        handle: thrussh::server::Handle,
        service: Service,
        urn: Urn,
    ) -> Result<(), anyhow::Error> {
        self.send
            .send(Incoming::ExecGit {
                channel,
                handle,
                service,
                urn,
            })
            .await
            .map_err(|_| anyhow::anyhow!("failed to send exec_git"))
    }

    pub(crate) async fn stop(&self) -> Result<(), anyhow::Error> {
        self.send
            .send(Incoming::Stop)
            .await
            .map_err(|_| anyhow::anyhow!("failed to send stop"))
    }
}

impl Processes {
    pub(crate) fn new(
        spawner: Arc<Spawner>,
        pool: Arc<Pool<Storage>>,
        rpc_socket_path: PathBuf,
    ) -> (Processes, ProcessesHandle) {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let processes = Processes {
            spawner,
            pool,
            incoming: rx,
            process_sends: HashMap::new(),
            running_processes: FuturesUnordered::new(),
            dead_sends: HashSet::new(),
            rpc_socket_path,
        };
        let handle = ProcessesHandle { send: tx };
        (processes, handle)
    }

    fn start(
        &mut self,
        id: thrussh::ChannelId,
        handle: thrussh::server::Handle,
        service: Service,
        urn: Urn,
    ) -> Result<(), anyhow::Error> {
        let process = git_shell::RunningGitShell::start(
            self.spawner.clone(),
            self.pool.clone(),
            id.clone(),
            handle,
            service,
            urn,
            self.rpc_socket_path.clone(),
        );
        self.running_processes.push(process.task);
        self.process_sends.insert(id, process.send);
        Ok(())
    }

    async fn send(&mut self, id: thrussh::ChannelId, data: Vec<u8>) -> Result<(), anyhow::Error> {
        self.process_sends
            .get(&id)
            .unwrap()
            .send(data)
            .await
            .map_err(anyhow::Error::from)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn run(mut self) -> Result<(), anyhow::Error> {
        loop {
            for id in &mut self.dead_sends.iter() {
                self.process_sends.remove(&id);
            }
            self.dead_sends.clear();
            let finished_process = &mut self.running_processes;
            futures::pin_mut!(finished_process);
            select! {
                completed_task = finished_process.next() => {
                    match completed_task {
                        Some(Ok((id, result))) => {
                            self.dead_sends.insert(id.clone());
                            match result {
                                Ok(()) => {
                                    tracing::info!(id=?id, "task finished");
                                },
                                Err(e) => panic!("task failed for channel {:?}: {}", id, e),
                            }
                        },
                        Some(Err(e)) => {
                            if e.is_panic() {
                                panic::resume_unwind(Box::new(e))
                            } else {
                                panic!("task cancelled whilst held by processes");
                            }
                        },
                        None => (),
                    }
                },
                new_incoming = self.incoming.recv().fuse() => {
                    if let Some(new_incoming) = new_incoming {
                        match new_incoming {
                            Incoming::ExecGit{service, urn, channel, handle} => {
                                tracing::trace!("starting git service");
                                self.start(channel, handle, service, urn)?;
                            },
                            Incoming::DataReceived{channel, data} => {
                                tracing::trace!("data received");
                                self.send(channel, data).await?;
                            },
                            Incoming::Stop => break,
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
