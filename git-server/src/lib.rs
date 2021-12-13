use std::sync::Arc;

use futures::StreamExt;
use structopt::StructOpt;
use tokio::net::TcpListener;

mod args;
mod config;
mod exec_str;
mod git_shell;
mod processes;
mod service;
mod server;

#[derive(thiserror::Error, Debug)]
pub enum RunError {
    #[error("could not open storage")]
    CouldNotOpenStorage,
    #[error("unable to bind to listen addr: {0}")]
    CouldNotBind(std::io::Error),
}

pub async fn main() {
    tracing_subscriber::fmt::init();
    let args = args::Args::from_args();
    let spawner = Arc::new(link_async::Spawner::from_current().unwrap());
    let config = match args.to_config(spawner.clone()).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{}", e);
            return;
        }
    };
    if let Err(e) = run(config, spawner).await {
        eprintln!("Error: {}", e);
    }
}

pub async fn run<S: librad::Signer + Clone>(
    config: config::Config<S>,
    spawner: Arc<link_async::Spawner>,
) -> Result<(), RunError> {
    let storage_pool = librad::git::storage::Pool::new(
        librad::git::storage::pool::ReadWriteConfig::new(
            config.paths.clone(),
            config.signer,
            librad::git::storage::pool::Initialised::no(),
        ),
        librad::net::peer::config::UserStorage::default().pool_size,
    );
    let mut thrussh_config = thrussh::server::Config::default();
    // TODO: store this somewhere so that we don't regenarate keys every time we startup, causing
    // SSH to have a tantrum about mismatching fingerprints. Should we automatically add it to
    // .ssh/known_hosts? Probably not, seems like a dodgy thing to do - plus I don't know how
    // that interacts with hostname hashing.
    let server_key = thrussh_keys::key::KeyPair::generate_ed25519().unwrap();
    thrussh_config.keys.push(server_key);
    let thrussh_config = Arc::new(thrussh_config);
    let storage = storage_pool
        .get()
        .await
        .map_err(|_| RunError::CouldNotOpenStorage)?;
    let peer_id = spawner.blocking(move || {
        storage.peer_id().clone()
    }).await;
    let sh = server::Server::new(
        spawner.clone(),
        peer_id.clone(),
        Arc::new(storage_pool),
        config.paths.rpc_socket(&peer_id),
    );
    let socket = TcpListener::bind(config.addr)
        .await
        .map_err(RunError::CouldNotBind)?;
    let ssh_tasks = sh.serve(&socket, thrussh_config).await;
    link_async::tasks::run_forever(ssh_tasks.boxed()).await;
    Ok(())
}
