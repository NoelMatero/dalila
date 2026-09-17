use std::cmp::Reverse;
use std::sync::{Arc, Mutex};

use tracing::{debug, warn};

use crate::node::LocalNode;
use crate::state::{MemberTable, MergeOutcome};
use crate::wire::WireMember;

/// How many rumors ride on one ping or ack. A rumor is roughly 30-45 bytes,
/// so this stays far below the datagram size limit.
pub const MAX_PIGGYBACK: usize = 10;

/// Each rumor is sent `RETRANSMIT_MULT * ceil(log2(cluster size))` times.
/// The log grows slowly, so big clusters don't need many more sends to reach
/// everyone.
const RETRANSMIT_MULT: u32 = 3;

struct Pending {
    rumor: WireMember,
    sends_left: u32,
}

/// Rumors this node still owes the cluster. Not the member table: the table
/// is what we believe, this is what we haven't finished telling people.
#[derive(Clone, Default)]
pub struct GossipQueue(Arc<Mutex<Vec<Pending>>>);

impl GossipQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a rumor, replacing any older one about the same node.
    pub fn push(&self, rumor: WireMember, cluster_size: usize) {
        let mut queue = self.0.lock().unwrap();
        queue.retain(|pending| pending.rumor.id != rumor.id);
        queue.push(Pending {
            rumor,
            sends_left: retransmit_limit(cluster_size),
        });
    }

    /// Rumors for the next outgoing packet. Each one taken counts as a send;
    /// rumors that have been sent enough times leave the queue.
    pub fn take(&self) -> Vec<WireMember> {
        let mut queue = self.0.lock().unwrap();

        // most sends left = sent the fewest times so far, i.e. the freshest news
        queue.sort_by_key(|pending| Reverse(pending.sends_left));

        let batch = queue
            .iter_mut()
            .take(MAX_PIGGYBACK)
            .map(|pending| {
                pending.sends_left -= 1;
                pending.rumor.clone()
            })
            .collect();

        queue.retain(|pending| pending.sends_left > 0);
        batch
    }
}

fn retransmit_limit(cluster_size: usize) -> u32 {
    // ceil(log2(n)), at least 1 so a rumor is always sent a few times
    let log = (cluster_size as u32).next_power_of_two().trailing_zeros();
    RETRANSMIT_MULT * log.max(1)
}

/// Merge a rumor, and pass on whatever it changed.
///
/// Every way news enters the table goes through here: joins, gossip, and the
/// detector's own findings. Whatever changes our table is news to the rest of
/// the cluster too, so it gets queued. A rumor that says we're not alive gets
/// answered with a newer "alive".
pub fn apply(
    node: &LocalNode,
    table: &MemberTable,
    queue: &GossipQueue,
    rumor: WireMember,
) -> MergeOutcome {
    let outcome = table.merge(rumor.clone(), node.id);

    // table.len() counts everyone but us
    let cluster_size = table.len() + 1;

    match outcome {
        MergeOutcome::Added | MergeOutcome::Updated => queue.push(rumor, cluster_size),
        MergeOutcome::Ignored => {}
        MergeOutcome::Refute { rumored } => {
            let before = node.incarnation();
            let now = node.refute(rumored);
            if now != before {
                warn!(state = ?rumor.state, %rumored, incarnation = %now, "a peer doubts we're alive; refuting");
            } else {
                // already beaten by our current incarnation (e.g. several copies
                // of the same rumor arriving at once). still re-announce it, in
                // case whoever sent this hasn't heard
                debug!(state = ?rumor.state, %rumored, incarnation = %now, "stale rumor about us");
            }
            queue.push(node.alive_rumor(), cluster_size);
        }
    }

    outcome
}
