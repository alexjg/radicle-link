use std::net::SocketAddr;

pub struct Config<S> {
    pub paths: librad::paths::Paths,
    pub signer: S,
    pub addr: SocketAddr,
}
