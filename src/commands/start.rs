use std::net::SocketAddr;

use anyhow::Ok;
use membership::join::{join, start_tcp_accept_loop};
use membership::node::LocalNode;
use membership::state::MemberTable;

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
    let table = MemberTable::new();

    let tmp_node = node.clone();
    let accept_table = table.clone();
    let tcp_loop = tokio::spawn(async move {
        start_tcp_accept_loop(node, accept_table).await.unwrap();
    });

    tokio::spawn(async move {
        if !cfg.seeds.is_empty() {
            join(cfg.seeds, tmp_node.clone()).await.unwrap();
        }
    });

    let _ = tcp_loop.await;

    Ok(())
}
