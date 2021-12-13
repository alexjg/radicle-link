use std::{path::PathBuf, net::SocketAddr, sync::Arc};

use librad::{
    crypto::BoxedSigner,
    profile::{Profile, RadHome},
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Args {
    pub rad_home: PathBuf,
    pub addr: SocketAddr,
}

impl Args {
    pub async fn to_config(
        self,
        spawner: Arc<link_async::Spawner>,
    ) -> Result<super::config::Config<BoxedSigner>, String> {
        let home = RadHome::Root(self.rad_home);
        let profile = Profile::from_home(&home, None)
            .map_err(|e| format!("unable to load profile: {}", e))?;
        let signer = spawner
            .blocking({
                let profile = profile.clone();
                move || {
                    rad_clib::keys::ssh::signer(&profile, rad_clib::keys::ssh::SshAuthSock::Env)
                        .map_err(|e| format!("unable to load ssh signing key: {}", e))
                }
            })
            .await?;
        Ok(super::config::Config {
            paths: profile.paths().clone(),
            signer,
            addr: self.addr,
        })
    }
}
