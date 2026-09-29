use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, bail};
use membership::detector::start_udp_detector;
use membership::dissemination::GossipQueue;
use membership::join::{join, start_tcp_accept_loop};
use membership::node::LocalNode;
use membership::state::MemberTable;
use membership::sync::start_sync_loop;
use membership::udp::{PendingAcks, announce_leave, start_udp_loop};
use tokio::net::{TcpListener, UdpSocket};
use tokio::signal::unix::{SignalKind, signal};
use tracing::info;

use crate::health::start_health_check;
use crate::proxy::{self, start_proxy};

pub struct Config {
    pub bind: SocketAddr,
    pub advertise: Option<SocketAddr>,
    pub seeds: Vec<SocketAddr>,
    /// Where this node's own app listens, to check it. `None`: not checked.
    pub backend_port: Option<u16>,
    pub proxy: Option<proxy::Config>,
}

impl From<crate::cli::StartArgs> for Config {
    fn from(args: crate::cli::StartArgs) -> Self {
        Self {
            bind: args.bind,
            advertise: args.advertise,
            seeds: args.join,
            backend_port: args.backend_port,
            // clap won't take --proxy without --backend-port, so this is only
            // None when there's no --proxy
            proxy: args
                .proxy
                .zip(args.backend_port)
                .map(|(listen, backend_port)| proxy::Config {
                    listen,
                    backend_port,
                }),
        }
    }
}

pub async fn execute(cfg: Config) -> anyhow::Result<()> {
    // every peer records us at the address we advertise, so it has to be one
    // they can connect to. checked before anything is bound, so a bad flag
    // fails straight away
    if cfg.advertise.unwrap_or(cfg.bind).ip().is_unspecified() {
        match cfg.advertise {
            Some(advertise) => {
                bail!("--advertise {advertise} isn't an address peers can connect to")
            }
            None => bail!(
                "--bind {} listens on every interface, which isn't an address peers can \
                 connect to. pass --advertise HOST:PORT with the one they should use, \
                 or --bind a specific address",
                cfg.bind
            ),
        }
    }

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

    // also before joining: if the proxy port is taken, fail now. finding out
    // after the join would mean leaving the cluster seconds after entering it
    let proxy_listener = match &cfg.proxy {
        Some(proxy) => Some(
            TcpListener::bind(proxy.listen)
                .await
                .with_context(|| format!("could not bind proxy {}", proxy.listen))?,
        ),
        None => None,
    };

    // without --advertise, the bound address. that has a specific IP (checked
    // above), and the real port even if --bind asked for port 0
    let node = LocalNode::new(cfg.advertise.unwrap_or(bound));
    let table = MemberTable::new();
    let queue = GossipQueue::new();
    let pending = PendingAcks::new();

    let accept_loop = tokio::spawn(start_tcp_accept_loop(
        listener,
        node.clone(),
        table.clone(),
        queue.clone(),
    ));
    let udp_loop = tokio::spawn(start_udp_loop(
        socket.clone(),
        node.clone(),
        table.clone(),
        queue.clone(),
        pending.clone(),
    ));
    info!(id = %node.id, bind = %bound, addr = %node.addr, "listening");

    // before joining, so the first check's answer has a chance to be in the
    // introduction the seed gets. if it isn't, it follows a second later by
    // gossip, like any other change
    let health = cfg.backend_port.map(|backend_port| {
        tokio::spawn(start_health_check(
            node.clone(),
            table.clone(),
            queue.clone(),
            backend_port,
        ))
    });

    if !cfg.seeds.is_empty() {
        join(cfg.seeds, &node, &table, &queue).await?;
    }

    // these start after the join, so the first round already has the cluster
    // in it. for the proxy, that means the first client isn't stuck with just us
    let proxy = match (proxy_listener, &cfg.proxy) {
        (Some(listener), Some(proxy)) => {
            info!(addr = %listener.local_addr()?, backend_port = proxy.backend_port, "proxying");
            Some(tokio::spawn(start_proxy(
                listener,
                node.clone(),
                table.clone(),
                proxy.backend_port,
            )))
        }
        _ => None,
    };
    let detector = tokio::spawn(start_udp_detector(
        socket.clone(),
        node.clone(),
        table.clone(),
        queue.clone(),
        pending,
    ));
    let sync_loop = tokio::spawn(start_sync_loop(node.clone(), table.clone(), queue));

    shutdown_signal().await?;
    info!("shutting down");

    // first, so nothing still running can refute our death from here on.
    // aborting doesn't reach tasks those loops spawned (a TCP connection
    // being served, a relayed probe), and any of them could hear the rumor
    node.leave();

    accept_loop.abort();
    udp_loop.abort();
    detector.abort();
    sync_loop.abort();
    if let Some(proxy) = proxy {
        proxy.abort();
    }
    if let Some(health) = health {
        health.abort();
    }

    // no need to linger afterwards: send_to returns once the OS has the
    // datagram, and exiting doesn't take it back
    let told = announce_leave(&socket, &node, &table).await;
    info!(told, "left the cluster");

    Ok(())
}

/// Wait for ctrl-c, or for SIGTERM: what `kill`, `systemctl stop` and
/// `docker stop` send.
async fn shutdown_signal() -> anyhow::Result<()> {
    let mut terminate = signal(SignalKind::terminate()).context("could not listen for SIGTERM")?;
    tokio::select! {
        ctrl_c = tokio::signal::ctrl_c() => ctrl_c.context("could not listen for ctrl-c"),
        _ = terminate.recv() => Ok(()),
    }
}
