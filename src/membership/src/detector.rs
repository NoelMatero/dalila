use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::{self, MissedTickBehavior};
use tracing::{debug, warn};

use crate::dissemination::{self, GossipQueue};
use crate::node::{LocalNode, Member, MemberState, NodeId};
use crate::state::{MemberTable, MergeOutcome};
use crate::udp::{self, PendingAcks};
use crate::wire::{WireMember, WireMemberState};

/// One protocol period: one probe per period.
pub const PROBE_INTERVAL: Duration = Duration::from_secs(1);

/// How long a member stays suspect before it is declared dead.
pub const SUSPICION_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn start_udp_detector(
    socket: Arc<UdpSocket>,
    node: LocalNode,
    table: MemberTable,
    queue: GossipQueue,
    pending: PendingAcks,
) {
    // 1. the list: filled lazily from the table, so members that join later are
    //    picked up on the next reshuffle
    let mut order: Vec<NodeId> = Vec::new();
    let mut seq: u32 = 0;

    // 2. the loop, one tick per protocol period. a slow tick pushes the next one
    //    back instead of firing a burst to catch up
    let mut ticker = time::interval(PROBE_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        // 3. a node from the shuffled list (5./7. next node, reshuffle at the end)
        if let Some(target) = next_target(&table, &mut order) {
            // 4. ping that node
            seq = seq.wrapping_add(1);
            let acked = udp::ping(&socket, &node, &queue, &pending, target.id, target.addr, seq).await;
            handle_result(&node, &table, &queue, &target, acked);
        }

        // 6. suspects nobody cleared in time are declared dead
        sweep_suspects(&node, &table, &queue);
    }
}

/// The next member to probe, skipping ones that are dead or no longer in the
/// table. Refills `order` with a fresh shuffle when it runs out.
fn next_target(table: &MemberTable, order: &mut Vec<NodeId>) -> Option<Member> {
    // at most two passes: what's left of this shuffle, then one fresh shuffle.
    // if both come up empty there is nobody to probe
    for _ in 0..2 {
        while let Some(id) = order.pop() {
            match table.get(&id) {
                Some(member) if member.state != MemberState::Dead => return Some(member),
                _ => continue,
            }
        }
        *order = table.shuffled_ids();
    }
    None
}

fn handle_result(
    node: &LocalNode,
    table: &MemberTable,
    queue: &GossipQueue,
    target: &Member,
    acked: bool,
) {
    if acked {
        // an ack at the same incarnation can't clear a suspicion. the suspect
        // clears itself: it hears the rumor by gossip and refutes it
        return;
    }

    debug!(id = %target.id, addr = %target.addr, "probe got no ack");
    // same incarnation as our entry, so this outranks Alive and is ignored if
    // the member is already suspect, which keeps its original clock
    if mark(node, table, queue, target, WireMemberState::Suspect) {
        warn!(id = %target.id, addr = %target.addr, "member suspected");
    }
}

fn sweep_suspects(node: &LocalNode, table: &MemberTable, queue: &GossipQueue) {
    let expired = table.members_where(|m| {
        matches!(m.state, MemberState::Suspect { since } if since.elapsed() >= SUSPICION_TIMEOUT)
    });

    for member in expired {
        if mark(node, table, queue, &member, WireMemberState::Dead) {
            warn!(id = %member.id, addr = %member.addr, "member declared dead");
        }
    }
}

/// Merge a new state for `member` at the incarnation we already have for it.
/// True if the table changed, in which case the rumor is also queued.
fn mark(
    node: &LocalNode,
    table: &MemberTable,
    queue: &GossipQueue,
    member: &Member,
    state: WireMemberState,
) -> bool {
    let rumor = WireMember {
        id: member.id,
        addr: member.addr,
        incarnation: member.incarnation,
        state,
    };
    dissemination::apply(node, table, queue, rumor) == MergeOutcome::Updated
}
