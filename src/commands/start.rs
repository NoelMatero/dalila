use std::net::SocketAddr;

use anyhow::Context;
use membership::join::{join, start_tcp_accept_loop};
use membership::node::LocalNode;
use membership::state::MemberTable;
use tokio::net::TcpListener;
use tracing::info;

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
    // bind before joining: once the seed has recorded us, it can hand our
    // address to the next node to join, which must find us listening
    let listener = TcpListener::bind(cfg.bind)
        .await
        .with_context(|| format!("could not bind {}", cfg.bind))?;

    // the address we actually got (differs from cfg.bind if port 0 was asked for)
    let node = LocalNode::new(listener.local_addr()?);
    let table = MemberTable::new();

    let accept_loop = tokio::spawn(start_tcp_accept_loop(
        listener,
        node.clone(),
        table.clone(),
    ));
    info!(id = %node.id, addr = %node.bind, "listening");

    if !cfg.seeds.is_empty() {
        join(cfg.seeds, &node, &table).await?;
    }

    tokio::signal::ctrl_c()
        .await
        .context("could not listen for ctrl-c")?;
    info!("shutting down");
    accept_loop.abort();

    Ok(())
}
