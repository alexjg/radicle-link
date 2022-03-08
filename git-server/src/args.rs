use std::{net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use librad::{
    crypto::BoxedSigner,
    profile::{LnkHome, Profile},
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Args {
    pub lnk_home: PathBuf,
    #[structopt(short)]
    pub addr: Option<SocketAddr>,
    #[structopt(long)]
    pub linger_timeout: Option<LingerTimeout>,
    #[structopt(long)]
    pub linkd_rpc_socket: Option<PathBuf>,
    #[structopt(long)]
    pub announce_on_push: bool,
}

impl Args {
    pub async fn to_config(
        self,
        spawner: Arc<link_async::Spawner>,
    ) -> Result<super::config::Config<BoxedSigner>, String> {
        let home = LnkHome::Root(self.lnk_home);
        let profile = Profile::from_home(&home, None)
            .map_err(|e| format!("unable to load profile: {}", e))?;
        let signer = spawner
            .blocking({
                let profile = profile.clone();
                move || {
                    lnk_clib::keys::ssh::signer(&profile, lnk_clib::keys::ssh::SshAuthSock::Env)
                        .map_err(|e| format!("unable to load ssh signing key: {}", e))
                }
            })
            .await?;
        Ok(super::config::Config {
            paths: profile.paths().clone(),
            signer,
            addr: self.addr,
            linger_timeout: self.linger_timeout.map(|l| l.into()),
            linkd_rpc_socket_path: self.linkd_rpc_socket,
            announce_on_push: self.announce_on_push,
        })
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct LingerTimeout(Duration);

impl From<LingerTimeout> for Duration {
    fn from(l: LingerTimeout) -> Self {
        l.0
    }
}

impl FromStr for LingerTimeout {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let integer: Result<u64, _> = s.parse();
        match integer {
            Ok(i) => Ok(LingerTimeout(Duration::from_millis(i))),
            Err(_) => Err("expected a positive integer"),
        }
    }
}
