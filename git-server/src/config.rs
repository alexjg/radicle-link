use std::{net::SocketAddr, path::PathBuf, time::Duration};

pub struct Config<S> {
    pub paths: librad::paths::Paths,
    pub signer: S,
    pub addr: Option<SocketAddr>,
    pub linger_timeout: Option<Duration>,
    pub linkd_rpc_socket_path: Option<PathBuf>,
    pub announce_on_push: bool,
}
