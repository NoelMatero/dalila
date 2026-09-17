use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::{
    dissemination::{self, GossipQueue},
    node::{LocalNode, NodeId},
    state::{MemberTable, MergeOutcome},
    wire::{WireIdentity, WireMember},
};

/// How long a probe waits for its ack before the target counts as unreachable.
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// A datagram is a whole message, so unlike TCP there is no length prefix.
/// Kept well under a typical MTU so a message is never split into IP fragments.
const MAX_DATAGRAM_LEN: usize = 1400;

#[derive(Serialize, Deserialize, Debug, Clone)]
enum UdpBody {
    Ping {
        /// Echoed back in the ack, so the prober can tell which ping it answers.
        seq: u32,
        from: WireIdentity,
        /// Rumors riding along. There is no separate gossip message.
        gossip: Vec<WireMember>,
    },

    Ack {
        seq: u32,
        from: WireIdentity,
        gossip: Vec<WireMember>,
    },
}

impl UdpBody {
    fn encode_frame(&self) -> anyhow::Result<Vec<u8>> {
        let payload = postcard::to_allocvec(self)?;
        Ok(payload)
    }
}

fn decode_frame(payload: &[u8]) -> Result<UdpBody> {
    let frame = postcard::from_bytes::<UdpBody>(payload)?;
    Ok(frame)
}

/// Probes waiting for an ack, by sequence number.
///
/// The detector registers a ping here before sending it, and the receive loop
/// completes it when the matching ack arrives. That is how two separate tasks
/// sharing one socket hand an answer to the one that asked.
#[derive(Clone, Default)]
pub struct PendingAcks(Arc<Mutex<HashMap<u32, (NodeId, oneshot::Sender<()>)>>>);

impl PendingAcks {
    pub fn new() -> Self {
        Self::default()
    }

    fn register(&self, seq: u32, target: NodeId) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.0.lock().unwrap().insert(seq, (target, tx));
        rx
    }

    /// Drop a probe that gave up. Does nothing if the ack already resolved it.
    fn forget(&self, seq: u32) {
        self.0.lock().unwrap().remove(&seq);
    }

    fn resolve(&self, seq: u32, from: NodeId) {
        let mut pending = self.0.lock().unwrap();

        // Only the node we pinged may answer. A different process now living
        // at the same address (a restart gets a new id) must not keep the old
        // member looking alive.
        if pending.get(&seq).is_some_and(|(target, _)| *target == from) {
            let (_, tx) = pending.remove(&seq).unwrap();
            // Err only means the prober already timed out and stopped waiting.
            let _ = tx.send(());
        }
    }
}

/// Ping `target` and wait for its ack. False on timeout or if the send failed.
pub async fn ping(
    socket: &UdpSocket,
    node: &LocalNode,
    queue: &GossipQueue,
    pending: &PendingAcks,
    target: NodeId,
    addr: SocketAddr,
    seq: u32,
) -> bool {
    // registered before sending, or a fast ack could arrive before anyone is waiting for it
    let ack = pending.register(seq, target);

    let body = UdpBody::Ping {
        seq,
        from: node.identity(),
        gossip: queue.take(),
    };
    if let Err(err) = send_udp(socket, addr, &body).await {
        warn!(%addr, "could not send ping: {err:#}");
        pending.forget(seq);
        return false;
    }

    let acked = matches!(timeout(PROBE_TIMEOUT, ack).await, Ok(Ok(())));
    pending.forget(seq);
    acked
}

async fn handle_ping_req(
    socket: &UdpSocket,
    node: &LocalNode,
    queue: &GossipQueue,
    seq: u32,
    addr: SocketAddr,
) -> Result<()> {
    // reply to the address the ping came from: that's where the prober's socket is
    let ack = UdpBody::Ack {
        seq,
        from: node.identity(),
        gossip: queue.take(),
    };
    send_udp(socket, addr, &ack).await
}

fn handle_ack_req(pending: &PendingAcks, seq: u32, from: WireIdentity) {
    pending.resolve(seq, from.id);
}

// apply every rumor a packet carried, before handling the packet itself
fn handle_gossip(node: &LocalNode, table: &MemberTable, queue: &GossipQueue, gossip: Vec<WireMember>) {
    for rumor in gossip {
        let (id, addr, state, incarnation) = (rumor.id, rumor.addr, rumor.state, rumor.incarnation);

        match dissemination::apply(node, table, queue, rumor) {
            MergeOutcome::Added => info!(%id, %addr, "new member (gossip)"),
            MergeOutcome::Updated => {
                info!(%id, %addr, ?state, %incarnation, "member updated (gossip)")
            }
            // refutations are logged inside apply
            MergeOutcome::Ignored | MergeOutcome::Refute { .. } => {}
        }
    }
}

async fn send_udp(socket: &UdpSocket, target: SocketAddr, body: &UdpBody) -> Result<()> {
    let payload = body.encode_frame()?;
    socket.send_to(&payload, target).await?;
    Ok(())
}

// receiving pings and acks
pub async fn start_udp_loop(
    socket: Arc<UdpSocket>,
    node: LocalNode,
    table: MemberTable,
    queue: GossipQueue,
    pending: PendingAcks,
) {
    let mut buf = [0u8; MAX_DATAGRAM_LEN];

    loop {
        let (len, addr) = match socket.recv_from(&mut buf).await {
            Ok(received) => received,
            Err(err) => {
                // some platforms report an earlier send's "port unreachable" here.
                // it's about one peer, not about this socket, so keep going
                debug!("udp receive failed: {err}");
                continue;
            }
        };

        // only the bytes that arrived, not the whole buffer
        let frame = match decode_frame(&buf[..len]) {
            Ok(frame) => frame,
            Err(err) => {
                warn!(%addr, "ignoring malformed datagram: {err:#}");
                continue;
            }
        };

        // gossip first: a rumor about us in this ping gets refuted, and the
        // refutation can then ride out on the ack we're about to send
        match frame {
            UdpBody::Ping {
                seq,
                from: _,
                gossip,
            } => {
                handle_gossip(&node, &table, &queue, gossip);
                if let Err(err) = handle_ping_req(&socket, &node, &queue, seq, addr).await {
                    warn!(%addr, "could not send ack: {err:#}");
                }
            }
            UdpBody::Ack { seq, from, gossip } => {
                handle_gossip(&node, &table, &queue, gossip);
                handle_ack_req(&pending, seq, from);
            }
        }
    }
}
