use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};

use rand::seq::{IteratorRandom, SliceRandom};

use crate::node::{Incarnation, Member, MemberState, NodeId};
use crate::wire::{WireMember, WireMemberState};

/// This node's belief about every peer it has heard of. Clones share the
/// same map.
///
/// `merge` is the only way in. Every source of news — a join, a probe result,
/// gossip, an expired timer — goes through the same rule, so the rule lives
/// in exactly one place.
#[derive(Clone, Default)]
pub struct MemberTable(Arc<Mutex<HashMap<NodeId, Member>>>);

/// What `merge` did with a rumor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeOutcome {
    /// Old news, or it agreed with what we already believed. Nothing changed,
    /// so there is nothing to pass on.
    Ignored,
    /// First we have heard of this member.
    Added,
    /// The rumor was newer than our entry and replaced it.
    Updated,
    /// The rumor is about this node and says it is not alive. The table
    /// can't answer that: only the local node may move its own incarnation
    /// past `rumored`.
    Refute { rumored: Incarnation },
}

impl MemberTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn merge(&self, rumor: WireMember, me: NodeId) -> MergeOutcome {
        if rumor.id == me {
            return match rumor.state {
                WireMemberState::Alive => MergeOutcome::Ignored,
                WireMemberState::Suspect | WireMemberState::Dead => MergeOutcome::Refute {
                    rumored: rumor.incarnation,
                },
            };
        }

        let mut members = self.0.lock().unwrap();
        match members.entry(rumor.id) {
            Entry::Vacant(slot) => {
                // a node never advertises 0.0.0.0 (it won't start without a
                // real address), but a record nobody can reach is worthless,
                // so refuse it rather than trust every peer to get that right
                if rumor.addr.ip().is_unspecified() {
                    return MergeOutcome::Ignored;
                }
                // never learn about a member by being told it's dead. either
                // we already reaped it, or we never knew it — and in both
                // cases we agree, so there is nothing to record.
                //
                // this is what makes reaping stick. without it, the first peer
                // to still hold the entry hands it back on the next sync, we
                // start a fresh grace period, and the two of us pass the
                // corpse back and forth for as long as the cluster runs
                if rumor.state == WireMemberState::Dead {
                    return MergeOutcome::Ignored;
                }
                slot.insert(rumor.into());
                MergeOutcome::Added
            }
            Entry::Occupied(mut slot) => {
                if supersedes(&rumor, slot.get()) {
                    // only incarnation, state and readiness change. the address
                    // stays as first learned: a process never moves (a restart
                    // gets a new id), and a rumor may carry an address that is
                    // worse than ours, like the 0.0.0.0 above
                    //
                    // readiness needs no rank of its own. only the member
                    // changes it, and always with a new incarnation, so two
                    // rumors at one incarnation never disagree about it
                    let member = slot.get_mut();
                    member.incarnation = rumor.incarnation;
                    member.state = rumor.state.into();
                    member.ready = rumor.ready;
                    MergeOutcome::Updated
                } else {
                    MergeOutcome::Ignored
                }
            }
        }
    }

    pub fn get(&self, id: &NodeId) -> Option<Member> {
        self.0.lock().unwrap().get(id).cloned()
    }

    pub fn contains(&self, id: &NodeId) -> bool {
        self.0.lock().unwrap().contains_key(id)
    }

    pub fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.lock().unwrap().is_empty()
    }

    /// A copy of every entry, taken under the lock and returned without it.
    pub fn snapshot(&self) -> Vec<Member> {
        self.0.lock().unwrap().values().cloned().collect()
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.0.lock().unwrap().keys().copied().collect()
    }

    /// Every id in random order: one pass of the probe rotation. The caller
    /// keeps its place and asks again when the pass runs out.
    pub fn shuffled_ids(&self) -> Vec<NodeId> {
        let mut ids = self.ids();
        ids.shuffle(&mut rand::rng());
        ids
    }

    /// Up to `count` members to relay a probe on our behalf, picked at random.
    ///
    /// `exclude` is the node being probed. A suspect is still worth asking:
    /// all we know about it is that it did not answer *us*, which is the same
    /// thing we are trying to find out about the target.
    pub fn relay_candidates(&self, exclude: &NodeId, count: usize) -> Vec<Member> {
        let mut candidates: Vec<Member> = self
            .0
            .lock()
            .unwrap()
            .values()
            .filter(|member| {
                member.id != *exclude && !matches!(member.state, MemberState::Dead { .. })
            })
            .cloned()
            .collect();

        candidates.shuffle(&mut rand::rng());
        candidates.truncate(count);
        candidates
    }

    /// One member picked at random to sync with, skipping the dead.
    pub fn random_member(&self) -> Option<Member> {
        self.0
            .lock()
            .unwrap()
            .values()
            .filter(|member| !matches!(member.state, MemberState::Dead { .. }))
            .choose(&mut rand::rng())
            .cloned()
    }

    /// Drop the given members from the table. Used to reap the long dead.
    ///
    /// Takes ids rather than a predicate so the decision is made outside the
    /// lock, which keeps the timing rule next to the other timing rules in
    /// the detector rather than buried in here.
    pub fn remove(&self, ids: &[NodeId]) {
        let mut members = self.0.lock().unwrap();
        for id in ids {
            members.remove(id);
        }
    }

    /// Every member believed alive whose app is answering: the set anything
    /// outside this crate wants when it asks "who can I send work to?"
    ///
    /// Suspects are left out. A suspect has already missed a direct probe and
    /// an indirect round, which is real evidence, and whichever way it turns
    /// out, it's settled in a few seconds.
    pub fn ready(&self) -> Vec<Member> {
        self.members_where(|m| matches!(m.state, MemberState::Alive) && m.ready)
    }

    pub fn members_where(&self, f: impl Fn(&Member) -> bool) -> Vec<Member> {
        self.0
            .lock()
            .unwrap()
            .values()
            .filter(|m| f(m))
            .cloned()
            .collect()
    }
}

/// A higher incarnation always wins. At the same incarnation,
/// Dead beats Suspect beats Alive.
///
/// That second half is what makes suspicion stick: a stale "alive" at the
/// same incarnation can't undo it. To clear itself, the suspect has to
/// publish a newer incarnation, and only the suspect can do that.
fn supersedes(rumor: &WireMember, existing: &Member) -> bool {
    let rumor_key = (rumor.incarnation, rank(rumor.state));
    let existing_key = (existing.incarnation, rank(existing.state.into()));
    rumor_key > existing_key
}

fn rank(state: WireMemberState) -> u8 {
    match state {
        WireMemberState::Alive => 0,
        WireMemberState::Suspect => 1,
        WireMemberState::Dead => 2,
    }
}
