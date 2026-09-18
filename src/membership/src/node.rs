use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use uuid::Uuid;

use crate::wire::{WireIdentity, WireMember, WireMemberState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(Uuid);

impl NodeId {
    pub fn random() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
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

impl fmt::Display for Incarnation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberState {
    Alive,
    /// `since` reads this machine's clock, taken when this node learned of the
    /// suspicion. Every node runs its own timer on the same rumor, which is
    /// why it never crosses the wire.
    Suspect {
        since: Instant,
    },
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
    /// The address actually bound. If port 0 was asked for, this holds the
    /// port the OS picked.
    pub bind: SocketAddr,
    /// Shared by every clone, like the member table. When one task refutes a
    /// rumor, every other task must advertise the new number from then on.
    incarnation: Arc<Mutex<Incarnation>>,
}

impl LocalNode {
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            id: NodeId::random(),
            bind,
            incarnation: Arc::new(Mutex::new(Incarnation::ZERO)),
        }
    }

    pub fn incarnation(&self) -> Incarnation {
        *self.incarnation.lock().unwrap()
    }

    /// How this node introduces itself to a peer. The port is included and
    /// the IP is not: the peer sees our IP on the connection, and `bind` may
    /// be 0.0.0.0, which nobody can connect to.
    pub fn identity(&self) -> WireIdentity {
        WireIdentity {
            id: self.id,
            port: self.bind.port(),
            incarnation: self.incarnation(),
        }
    }

    /// Answer a rumor that this node is suspect or dead: move our incarnation
    /// past it, so our "alive" outranks it everywhere. Returns the incarnation
    /// to advertise from now on.
    pub fn refute(&self, rumored: Incarnation) -> Incarnation {
        let mut current = self.incarnation.lock().unwrap();
        // a rumor older than our current incarnation is already beaten by it
        if rumored >= *current {
            *current = Incarnation::superseding(rumored);
        }
        *current
    }

    /// "This node is alive", as a rumor to gossip.
    ///
    /// Carries `bind` as the address, which may be 0.0.0.0. That's fine for
    /// nodes that already know us, since merge keeps the address it has.
    pub fn alive_rumor(&self) -> WireMember {
        WireMember {
            id: self.id,
            addr: self.bind,
            incarnation: self.incarnation(),
            state: WireMemberState::Alive,
        }
    }
}
