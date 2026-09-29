use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// `since` works the same way, and drives the second timer: once a dead
    /// entry is old enough, it is dropped from the table entirely.
    Dead {
        since: Instant,
    },
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
    /// Where peers reach this node. Always a real address, never 0.0.0.0:
    /// this is what every peer records for us, so it has to be something
    /// they can connect to.
    pub addr: SocketAddr,
    /// Shared by every clone, like the member table. When one task refutes a
    /// rumor, every other task must advertise the new number from then on.
    incarnation: Arc<Mutex<Incarnation>>,
    /// Set once this node is shutting down on purpose. From then on it lets
    /// rumors of its death stand instead of refuting them.
    ///
    /// Only read or written while holding `incarnation`'s lock, so a refute
    /// is either finished before `leave` or never happens. An atomic only
    /// because it lives outside that mutex.
    leaving: Arc<AtomicBool>,
}

impl LocalNode {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            id: NodeId::random(),
            addr,
            incarnation: Arc::new(Mutex::new(Incarnation::ZERO)),
            leaving: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn incarnation(&self) -> Incarnation {
        *self.incarnation.lock().unwrap()
    }

    /// How this node introduces itself to a peer.
    pub fn identity(&self) -> WireIdentity {
        WireIdentity {
            id: self.id,
            addr: self.addr,
            incarnation: self.incarnation(),
        }
    }

    /// Answer a rumor that this node is suspect or dead: move our incarnation
    /// past it, so our "alive" outranks it everywhere. Returns the incarnation
    /// to advertise from now on, or `None` if this node is leaving and the
    /// rumor should stand.
    pub fn refute(&self, rumored: Incarnation) -> Option<Incarnation> {
        let mut current = self.incarnation.lock().unwrap();
        if self.leaving.load(Ordering::Relaxed) {
            return None;
        }
        // a rumor older than our current incarnation is already beaten by it
        if rumored >= *current {
            *current = Incarnation::superseding(rumored);
        }
        Some(*current)
    }

    /// Stop defending this node's liveness. Its incarnation is final from
    /// here on, so a "dead" at that incarnation outranks every "alive" it
    /// ever sent.
    pub fn leave(&self) {
        // under the lock: a refute already past its check finishes first, and
        // the number we announce our death at can't move after this
        let _current = self.incarnation.lock().unwrap();
        self.leaving.store(true, Ordering::Relaxed);
    }

    /// "This node is alive", as a rumor to gossip.
    pub fn alive_rumor(&self) -> WireMember {
        WireMember {
            id: self.id,
            addr: self.addr,
            incarnation: self.incarnation(),
            state: WireMemberState::Alive,
        }
    }
}
