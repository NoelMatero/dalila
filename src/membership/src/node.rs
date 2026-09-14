use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::time::Instant;
use uuid::Uuid;

use crate::wire::{FromTwo, WireIdentity};

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

    pub fn superseding(other: Self) -> Self {
        Self(other.0.saturating_add(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberState {
    Alive,
    Suspect { since: Option<Instant> },
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

#[derive(Debug, Clone)]
pub struct LocalNode {
    pub id: NodeId,
    pub bind: SocketAddr,
    pub incarnation: Incarnation,
}

impl LocalNode {
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            id: NodeId::random(),
            bind,
            incarnation: Incarnation::ZERO,
        }
    }

    pub fn refute(&mut self, rumored: Incarnation) {
        self.incarnation = Incarnation::superseding(rumored.max(self.incarnation));
    }
}

impl From<LocalNode> for Member {
    fn from(local_node: LocalNode) -> Member {
        Member {
            id: local_node.id,
            addr: local_node.bind,
            incarnation: local_node.incarnation,
            state: MemberState::Alive,
        }
    }
}

// Converters moved here from wire.rs to break the loop:
/// A peer that just announced itself: the IP is observed from the connection,
/// the port is what it told us it listens on.
impl FromTwo<WireIdentity, SocketAddr> for Member {
    fn from_two(identity: WireIdentity, addr: SocketAddr) -> Self {
        Member {
            id: identity.id,
            addr: SocketAddr::new(addr.ip(), identity.port),
            incarnation: identity.incarnation,
            state: MemberState::Alive,
        }
    }
}

impl From<LocalNode> for WireIdentity {
    fn from(node: LocalNode) -> Self {
        WireIdentity {
            id: node.id,
            port: node.bind.port(),
            incarnation: node.incarnation,
        }
    }
}
