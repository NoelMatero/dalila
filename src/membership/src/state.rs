use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};

use rand::seq::SliceRandom;

use crate::node::{Incarnation, Member, NodeId};
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
                slot.insert(rumor.into());
                MergeOutcome::Added
            }
            Entry::Occupied(mut slot) => {
                if supersedes(&rumor, slot.get()) {
                    slot.insert(rumor.into());
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
