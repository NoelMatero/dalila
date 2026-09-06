use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(Uuid);

impl NodeId {
    pub fn random() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct Incarnation(u32);

impl Incarnation {
    pub const ZERO: Self = Self(0);

    // step past a rumor in order to outrank it
    pub fn superseding(other: Self) -> Self {
        Self(other.0.saturating_add(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberState {
    Alive,
    Suspect { since: Instant },
    Dead,
}

#[derive(Debug, Clone)]
pub struct Member {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub incarnation: Incarnation,
    pub state: MemberState,
}

impl Member {
    pub fn alive(id: NodeId, addr: SocketAddr, incarnation: Incarnation) -> Self {
        Self {
            id,
            addr,
            incarnation,
            state: MemberState::Alive,
        }
    }
}

#[derive(Debug)]
pub struct LocalNode {
    pub id: NodeId,
    pub bind: SocketAddr,
    pub incarnation: Incarnation,
    pub members: Vec<Member>,
}

impl LocalNode {
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            id: NodeId::random(),
            bind,
            incarnation: Incarnation::ZERO,
            members: Vec::new(),
        }
    }

    // nswer a rumor that this node is suspect or dead by outranking it
    pub fn refute(&mut self, rumored: Incarnation) {
        self.incarnation = Incarnation::superseding(rumored.max(self.incarnation));
    }
}
