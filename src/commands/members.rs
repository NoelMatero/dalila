use std::net::SocketAddr;

use anyhow::Result;

pub struct Config {
    pub addr: SocketAddr,
}

impl From<crate::cli::MembersArgs> for Config {
    fn from(args: crate::cli::MembersArgs) -> Self {
        Self { addr: args.addr }
    }
}

pub async fn execute(_cfg: Config) -> Result<()> {
    // TODO: connect to the running agent, request its member table, print it.
    todo!()
}
