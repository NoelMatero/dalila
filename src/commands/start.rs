use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use membership::detector::start_udp_detector;
use membership::join::{join, start_tcp_accept_loop};
use membership::node::LocalNode;
use membership::state::MemberTable;
use membership::udp::{PendingAcks, start_udp_loop};
use tokio::net::{TcpListener, UdpSocket};
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
    let bound = listener.local_addr()?;

    // same address and port as TCP. tcp/7946 and udp/7946 are separate
    // endpoints, and peers only know one port for us
    let socket = UdpSocket::bind(bound)
        .await
        .with_context(|| format!("could not bind udp {bound}"))?;
    // one socket, shared by the receive loop and the detector. send_to and
    // recv_from take &self, so both can use it at once through the Arc
    let socket = Arc::new(socket);

    let node = LocalNode::new(bound);
    let table = MemberTable::new();
    let pending = PendingAcks::new();

    let accept_loop = tokio::spawn(start_tcp_accept_loop(
        listener,
        node.clone(),
        table.clone(),
    ));
    let udp_loop = tokio::spawn(start_udp_loop(
        socket.clone(),
        node.clone(),
        pending.clone(),
    ));
    info!(id = %node.id, addr = %node.bind, "listening");

    if !cfg.seeds.is_empty() {
        join(cfg.seeds, &node, &table).await?;
    }

    // probing starts after the join, so the first shuffle already has the cluster in it
    let detector = tokio::spawn(start_udp_detector(socket, node, table, pending));

    tokio::signal::ctrl_c()
        .await
        .context("could not listen for ctrl-c")?;
    info!("shutting down");
    accept_loop.abort();
    udp_loop.abort();
    detector.abort();

    Ok(())
}
