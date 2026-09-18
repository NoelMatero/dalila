use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
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
    node::{LocalNode, Member, NodeId},
    state::{MemberTable, MergeOutcome},
    wire::{WireIdentity, WireMember},
};

/// How long a probe waits for its ack before the target counts as unreachable.
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// How long the indirect round waits for the first relay to report back.
///
/// Longer than `PROBE_TIMEOUT`, because a relay spends up to that long on its
/// own probe before it has anything to say, and its answer still has to travel
/// back to us. A relay that answers later than this is simply too late: we have
/// already moved on and suspected the target, and gossip will sort it out.
pub const INDIRECT_TIMEOUT: Duration = Duration::from_millis(600);

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

    /// "Our own probe of `target` got nothing. Try it for us."
    PingReq {
        /// The asker's sequence number, echoed back in the `PingReqAck`. The
        /// relay's own probe of the target uses a different one.
        seq: u32,
        from: WireIdentity,
        /// Who to probe. No address: the relay looks it up in its own table,
        /// since the relay is the one that has to get a packet there.
        target: NodeId,
        gossip: Vec<WireMember>,
    },

    /// "Your target answered me." Only sent when the relayed probe succeeded;
    /// a failed one says nothing, which is the same as being unreachable.
    PingReqAck {
        seq: u32,
        /// The relay. Not the node being vouched for.
        from: WireIdentity,
        /// The node that answered, which is what the asker was waiting on.
        target: NodeId,
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

/// Probes waiting for an answer, by sequence number, and the source of those
/// sequence numbers.
///
/// The prober registers a probe here before sending it, and the receive loop
/// completes it when the matching answer arrives. That is how two separate
/// tasks sharing one socket hand an answer to the one that asked.
/// Probes in flight, keyed by sequence number: the node each one is asking
/// about, and the channel its answer should arrive on.
type Waiting = HashMap<u32, (NodeId, oneshot::Sender<()>)>;

#[derive(Clone, Default)]
pub struct PendingAcks {
    waiting: Arc<Mutex<Waiting>>,
    next_seq: Arc<AtomicU32>,
}

impl PendingAcks {
    pub fn new() -> Self {
        Self::default()
    }

    /// A sequence number no other probe in flight on this node is using.
    ///
    /// The detector is no longer the only thing probing: a relayed probe
    /// starts from the receive loop. Both draw from here, so the two can't
    /// pick the same number and resolve each other's waits.
    pub fn next_seq(&self) -> u32 {
        self.next_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Wait for an answer about `subject`. The subject is the node whose
    /// liveness is in question, which for a relayed probe is not the node
    /// that will send the answer.
    fn register(&self, seq: u32, subject: NodeId) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.waiting.lock().unwrap().insert(seq, (subject, tx));
        rx
    }

    /// Drop a probe that gave up. Does nothing if the answer already resolved it.
    fn forget(&self, seq: u32) {
        self.waiting.lock().unwrap().remove(&seq);
    }

    fn resolve(&self, seq: u32, subject: NodeId) {
        let mut waiting = self.waiting.lock().unwrap();

        // Only an answer about the node we asked about counts. A different
        // process now living at the same address (a restart gets a new id)
        // must not keep the old member looking alive.
        if waiting
            .get(&seq)
            .is_some_and(|(asked, _)| *asked == subject)
        {
            let (_, tx) = waiting.remove(&seq).unwrap();
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

/// Ask `relays` to probe `target` for us, and wait for the first one that
/// reports back. False if none of them does in time.
pub async fn ping_req(
    socket: &UdpSocket,
    node: &LocalNode,
    queue: &GossipQueue,
    pending: &PendingAcks,
    target: NodeId,
    relays: &[Member],
    seq: u32,
) -> bool {
    // one entry for the whole round, under the asker's seq. the first relay to
    // report resolves it; the ones after that find nothing registered and are
    // dropped, which is exactly what we want. we only need one witness
    let ack = pending.register(seq, target);

    let mut sent = 0;
    for relay in relays {
        // gossip is taken per relay: these are separate packets to separate
        // peers, so each one is a real send and counts as one
        let body = UdpBody::PingReq {
            seq,
            from: node.identity(),
            target,
            gossip: queue.take(),
        };

        match send_udp(socket, relay.addr, &body).await {
            Ok(()) => sent += 1,
            Err(err) => warn!(addr = %relay.addr, "could not send ping-req: {err:#}"),
        }
    }

    if sent == 0 {
        pending.forget(seq);
        return false;
    }

    let acked = matches!(timeout(INDIRECT_TIMEOUT, ack).await, Ok(Ok(())));
    pending.forget(seq);
    acked
}

async fn handle_ping(
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

/// Probe `target` on someone else's behalf, and report back only if it answers.
///
/// Runs in its own task: it spends up to `PROBE_TIMEOUT` waiting, and the
/// receive loop it was started from handles one datagram at a time.
// socket/node/table/queue/pending travel together through most of this crate;
// the real fix is one context struct holding all five, which is a refactor of
// its own rather than part of this change
#[allow(clippy::too_many_arguments)]
async fn handle_ping_req(
    socket: &UdpSocket,
    node: &LocalNode,
    table: &MemberTable,
    queue: &GossipQueue,
    pending: &PendingAcks,
    seq: u32,
    target: NodeId,
    asker: SocketAddr,
) {
    // the asker sent an id, not an address. if we've never heard of this node
    // we have nowhere to send a probe, and saying nothing is the right answer
    let Some(member) = table.get(&target) else {
        debug!(%target, "asked to relay a probe to a node we don't know");
        return;
    };

    // an ordinary probe: the target can't tell it was asked for by someone else
    let relayed = pending.next_seq();
    if !ping(socket, node, queue, pending, target, member.addr, relayed).await {
        debug!(%target, addr = %member.addr, "relayed probe got no ack either");
        return;
    }

    let body = UdpBody::PingReqAck {
        seq,
        from: node.identity(),
        target,
        gossip: queue.take(),
    };
    if let Err(err) = send_udp(socket, asker, &body).await {
        warn!(addr = %asker, "could not report a relayed probe: {err:#}");
    }
}

/// Complete whatever probe was waiting on news about `subject`.
fn handle_ack(pending: &PendingAcks, seq: u32, subject: NodeId) {
    pending.resolve(seq, subject);
}

// apply every rumor a packet carried, before handling the packet itself
fn handle_gossip(
    node: &LocalNode,
    table: &MemberTable,
    queue: &GossipQueue,
    gossip: Vec<WireMember>,
) {
    for rumor in gossip {
        let (id, addr, state, incarnation) = (rumor.id, rumor.addr, rumor.state, rumor.incarnation);

        match dissemination::apply(node, table, queue, rumor) {
            MergeOutcome::Added => {
                let effective_addr = table.get(&id).map(|m| m.addr).unwrap_or(addr);
                info!(%id, addr = %effective_addr, "new member (gossip)");
            }
            MergeOutcome::Updated => {
                let effective_addr = table.get(&id).map(|m| m.addr).unwrap_or(addr);
                info!(%id, addr = %effective_addr, ?state, %incarnation, "member updated (gossip)");
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
                if let Err(err) = handle_ping(&socket, &node, &queue, seq, addr).await {
                    warn!(%addr, "could not send ack: {err:#}");
                }
            }
            UdpBody::Ack { seq, from, gossip } => {
                handle_gossip(&node, &table, &queue, gossip);
                // a direct ack speaks for whoever sent it
                handle_ack(&pending, seq, from.id);
            }
            UdpBody::PingReq {
                seq,
                from: _,
                target,
                gossip,
            } => {
                handle_gossip(&node, &table, &queue, gossip);

                // relaying means waiting on a probe of our own, and this loop
                // handles one datagram at a time. doing it here would stall
                // every other packet for half a second, including our own acks
                // — which would make this node look dead to everyone else.
                // a spawned task has to own what it touches, hence the clones;
                // every one of them is an Arc inside, so it's a refcount bump
                let socket = socket.clone();
                let node = node.clone();
                let table = table.clone();
                let queue = queue.clone();
                let pending = pending.clone();

                tokio::spawn(async move {
                    handle_ping_req(&socket, &node, &table, &queue, &pending, seq, target, addr)
                        .await;
                });
            }
            UdpBody::PingReqAck {
                seq,
                from,
                target,
                gossip,
            } => {
                handle_gossip(&node, &table, &queue, gossip);
                debug!(relay = %from.id, %target, "a relay reached the node we couldn't");
                // a relayed ack speaks for the target, not for the relay that
                // carried it. that is the whole point of the round
                handle_ack(&pending, seq, target);
            }
        }
    }
}
