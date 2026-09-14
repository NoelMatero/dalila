use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rand::seq::SliceRandom;

use crate::node::{Member, MemberState, NodeId};

#[derive(Clone, Default)]
pub struct MemberTable(Arc<Mutex<HashMap<NodeId, Member>>>);

impl MemberTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_members(members: impl IntoIterator<Item = Member>) -> Self {
        let table = Self::new();
        for member in members {
            table.insert(member);
        }
        table
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

    /// A point-in-time copy of every record. Nothing holds the lock afterwards.
    pub fn snapshot(&self) -> Vec<Member> {
        self.0.lock().unwrap().values().cloned().collect()
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.0.lock().unwrap().keys().copied().collect()
    }

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

    // TODO: none of these compare incarnations. The merge rule — "does this
    // rumor supersede what I already believe?" — belongs here, as the single
    // entry point every other module calls. Once it exists, `insert` and
    // `set_state` should stop being public.

    pub fn insert(&self, member: Member) {
        self.0.lock().unwrap().insert(member.id, member);
    }

    pub fn set_state(&self, id: &NodeId, state: MemberState) -> Option<()> {
        self.0.lock().unwrap().get_mut(id).map(|m| m.state = state)
    }

    pub fn remove(&self, id: &NodeId) -> Option<Member> {
        self.0.lock().unwrap().remove(id)
    }
}
