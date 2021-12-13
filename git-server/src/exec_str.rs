use librad::git::Urn;
use radicle_git_ext as git_ext;

use crate::service::Service;

pub(crate) fn parse_exec_str(exec_str: &str) -> Result<(Service, Urn), anyhow::Error> {
    let re = regex::Regex::new(r"(\S+) '/(.+)'").unwrap();
    let cap = re.captures_iter(exec_str).next().unwrap();
    let service_str: &str = &cap[1];
    let urn_str = &cap[2];

    let urn = parse_urn(urn_str).unwrap();
    let service = service_str.parse().unwrap();
    Ok((service, urn))
}

fn parse_urn(urn_str: &str) -> Result<Urn, anyhow::Error> {
    let bytes = multibase::decode(urn_str).map(|(_base, bytes)| bytes)?;
    let mhash = multihash::Multihash::from_bytes(bytes)?;
    let oid = git_ext::Oid::try_from(mhash)?;
    Ok(oid.into())
}
