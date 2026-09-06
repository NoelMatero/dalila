use std::net::SocketAddr;

use anyhow::{Ok, Result};
use membership::join::{self, join, start_tcp_accept_loop};
use membership::node::LocalNode;
use tokio::net::tcp;

pub struct Config {
    pub bind: SocketAddr,
    pub seeds: Vec<SocketAddr>,
}

impl From<crate::cli::StartArgs> for Config {
    fn from(args: crate::cli::StartArgs) -> Self {
        Self {
            bind: args.bind,
            seeds: args.join,
        }
    }
}

pub async fn execute(cfg: Config) -> anyhow::Result<()> {
    let node = LocalNode::new(cfg.bind);

    let tcp_loop = tokio::spawn(async move {
        start_tcp_accept_loop(cfg.bind, node.members).await.unwrap();
    });

    tokio::spawn(async move {
        if !cfg.seeds.is_empty() {
            join(cfg.seeds, node).await.unwrap();
        }
    });

    let _ = tcp_loop.await;

    println!("we got this far");

    Ok(())

    // TODO: bind and start accepting BEFORE joining, then:
}
