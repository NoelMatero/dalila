use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::time::{self, MissedTickBehavior, timeout};
use tracing::{debug, info};

use crate::dissemination::{self, GossipQueue};
use crate::join::{IO_TIMEOUT, TcpConnection};
use crate::node::{LocalNode, Member};
use crate::state::{MemberTable, MergeOutcome};
use crate::wire::{TcpBody, WireIdentity, WireMember};

/// How often a node picks one peer and trades whole tables with it.
///
/// Much slower than the probe interval, because this is a backstop and not
/// the main road. Gossip is what spreads news in a second or two; this is
/// what fixes the cases gossip silently lost, and those are rare enough that
/// checking ten times a minute is plenty.
pub const SYNC_INTERVAL: Duration = Duration::from_secs(10);

/// Trade full member tables with one random peer, over and over.
///
/// Gossip is best effort in a strong sense: a rumor is retransmitted a fixed
/// number of times and then dropped from the queue forever. Nothing retries
/// it, and nothing notices it was lost. If every copy of some rumor missed a
/// node — a run of dropped datagrams, a node that was partitioned while the
/// rumor was live, a queue that overflowed `MAX_PIGGYBACK` — that node's
/// table is simply wrong, and stays wrong.
///
/// This loop is the repair. It never compares tables or computes a delta; it
/// just re-states everything both sides believe and lets `merge` throw away
/// whatever is older. That's the trick: because the merge rule is
/// order-independent and a repeat is a no-op, saying everything again is
/// always safe and always enough.
pub async fn start_sync_loop(node: LocalNode, table: MemberTable, queue: GossipQueue) {
    let mut ticker = time::interval(SYNC_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        // nobody to sync with: a lone node, or everyone we know is dead
        let Some(partner) = table.random_member() else {
            continue;
        };

        if let Err(err) = sync_with(&partner, &node, &table, &queue).await {
            // a peer that's down is the detector's business, not ours. this
            // loop is a repair pass, and a failed pass just means the next
            // one picks someone else
            debug!(id = %partner.id, addr = %partner.addr, "sync failed: {err:#}");
        }
    }
}

/// One round trip: everything we believe for everything they believe.
async fn sync_with(
    partner: &Member,
    node: &LocalNode,
    table: &MemberTable,
    queue: &GossipQueue,
) -> Result<()> {
    let mut tcp_connection = TcpConnection::connect(partner.addr).await?;

    tcp_connection
        .write_frame(&TcpBody::SyncRequest {
            from: node.identity(),
            members: table.snapshot().into_iter().map(Into::into).collect(),
        })
        .await?;

    let reply = timeout(IO_TIMEOUT, tcp_connection.read_frame())
        .await
        .context("timed out waiting for the sync response")??;

    match reply {
        Some(TcpBody::SyncResponse { from, members }) => {
            absorb(node, table, queue, from, members);
            Ok(())
        }
        Some(_) => bail!("peer replied with something other than a sync response"),
        None => bail!("peer closed the connection without replying"),
    }
}

/// Merge a peer's whole table, plus the peer's own record.
///
/// The peer has no entry for itself in what it sent, the same way we have
/// none for ourselves, so its identity is turned into a record here, exactly
/// as a join does it.
pub fn absorb(
    node: &LocalNode,
    table: &MemberTable,
    queue: &GossipQueue,
    from: WireIdentity,
    members: Vec<WireMember>,
) {
    apply(node, table, queue, from.into_member());

    for member in members {
        apply(node, table, queue, member);
    }
}

// merge one rumor, and say so if it turned out to be news
fn apply(node: &LocalNode, table: &MemberTable, queue: &GossipQueue, rumor: WireMember) {
    let (id, addr, state) = (rumor.id, rumor.addr, rumor.state);

    // anything this taught us is queued by `apply`, so a repair doesn't stop
    // with us: the rumor we were missing goes back out by the usual route
    match dissemination::apply(node, table, queue, rumor) {
        MergeOutcome::Added => info!(%id, %addr, "new member (sync)"),
        MergeOutcome::Updated => info!(%id, %addr, ?state, "member updated (sync)"),
        MergeOutcome::Ignored | MergeOutcome::Refute { .. } => {}
    }
}
