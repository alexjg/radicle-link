use lazy_static::lazy_static;
use tracing::instrument;

use librad::git::Urn;
use radicle_git_ext as git_ext;

use crate::service::Service;

#[derive(thiserror::Error, Debug)]
pub(crate) enum ParseError {
    #[error("the exec str must be in the form <service> <urn>")]
    Format,
    #[error("invalid URN")]
    Urn,
    #[error("invalid service")]
    Service,
}

lazy_static! {
    static ref SERVICE_REGEX: regex::Regex = regex::Regex::new(r"(\S+) '/(.+)'").unwrap();
}

#[instrument]
pub(crate) fn parse_exec_str(exec_str: &str) -> Result<(Service, Urn), ParseError> {
    let cap = SERVICE_REGEX
        .captures_iter(exec_str)
        .next()
        .ok_or(ParseError::Format)?;
    debug_assert!(cap.len() == 2);
    let service_str: &str = &cap[1];
    let urn_str = &cap[2];

    let urn = parse_urn(urn_str)?;
    let service = service_str.parse().map_err(|_| {
        tracing::error!("invalid service");
        ParseError::Service
    })?;
    Ok((service, urn))
}

fn parse_urn(urn_str: &str) -> Result<Urn, ParseError> {
    let bytes = multibase::decode(urn_str)
        .map(|(_base, bytes)| bytes)
        .map_err(|e| {
            tracing::error!(err=?e, "invalid multibase when decoding URN");
            ParseError::Urn
        })?;
    let mhash = multihash::Multihash::from_bytes(bytes).map_err(|e| {
        tracing::error!(err=?e, "invalid multihash when decoding URN");
        ParseError::Urn
    })?;
    let oid = git_ext::Oid::try_from(mhash).map_err(|_| ParseError::Urn)?;
    Ok(oid.into())
}
