use std::{fmt::Debug, str::FromStr};

use git2::transport::Service as GitService;

#[derive(Clone, Copy)]
pub(crate) struct Service(pub GitService);

impl From<GitService> for Service {
    fn from(g: GitService) -> Self {
        Service(g) 
    }
}

impl From<Service> for GitService {
    fn from(s: Service) -> Self {
        s.0 
    }
}

#[derive(Debug, thiserror::Error)]
#[error("not a valid service name")]
pub(crate) struct ParseError{}

impl FromStr for Service {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "git-upload-pack" => Ok(Service(GitService::UploadPack)),
            "git-upload-pack-ls" => Ok(Service(GitService::UploadPackLs)),
            "git-receive-pack" => Ok(Service(GitService::ReceivePack)),
            "git-receive-pack-ls" => Ok(Service(GitService::ReceivePackLs)),
            _ => Err(ParseError{})
        }
    }
}

impl Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_tuple("Service")
            .field(match self.0 {
                GitService::UploadPackLs => &"UploadPackLs",
                GitService::UploadPack => &"UploadPack",
                GitService::ReceivePackLs => &"ReceivePackLs",
                GitService::ReceivePack => &"ReceivePack",
            })
            .finish()
    }
}

impl std::fmt::Display for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            GitService::UploadPack => write!(f, "upload-pack"),
            GitService::UploadPackLs => write!(f, "upload-pack-ls"),
            GitService::ReceivePack => write!(f, "receive-pack"),
            GitService::ReceivePackLs => write!(f, "receive-pack-ls"),
        }
    }
}

impl PartialEq for Service {
    #[allow(clippy::match_like_matches_macro)]
    fn eq(&self, other: &Self) -> bool {
        match (self.0, other.0) {
            (GitService::UploadPackLs, GitService::UploadPackLs) => true,
            (GitService::UploadPack, GitService::UploadPack) => true,
            (GitService::ReceivePackLs, GitService::ReceivePackLs) => true,
            (GitService::ReceivePack, GitService::ReceivePack) => true,
            _ => false,
        }
    }
}
