use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use uuid::Uuid;

use crate::auth::ClusterKey;
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
    /// Whether the member's app answered its own last check. Separate from
    /// `state`: a member whose app is down is still a member, and still gets
    /// probed. It just shouldn't be sent work.
    pub ready: bool,
    /// What the member says it runs, like `web` or `api`. A proxy can be told
    /// to send work only to members with a given tag.
    pub tags: Vec<String>,
}

impl Member {
    pub fn alive(id: NodeId, addr: SocketAddr, incarnation: Incarnation) -> Self {
        Self {
            id,
            addr,
            incarnation,
            state: MemberState::Alive,
            ready: true,
            tags: Vec::new(),
        }
    }
}

/// This node's own state: everything it says about itself, and whether it
/// has stopped saying it. One lock for all of it, so a rumor never pairs a
/// new incarnation with an old field, and a change can't slip in between
/// `leave` and the death it announces.
#[derive(Debug)]
struct Own {
    incarnation: Incarnation,
    /// Set once this node is shutting down on purpose. From then on it lets
    /// rumors of its death stand instead of refuting them, and changes
    /// nothing else about itself.
    leaving: bool,
    /// Whether this node's own app answered its last check. Starts true, and
    /// stays true on a node that doesn't check.
    app_up: bool,
    /// Taken out of rotation on purpose, through the control API. The app
    /// may be fine; this node just shouldn't be sent work for now.
    drained: bool,
    /// What this node runs, like `web`. Starts from `--tag`.
    tags: Vec<String>,
}

impl Own {
    /// What peers are told about whether to send us work.
    fn ready(&self) -> bool {
        self.app_up && !self.drained
    }
}

/// A copy of this node's own state, for the control API to show.
#[derive(Debug, Clone)]
pub struct Status {
    pub incarnation: Incarnation,
    pub ready: bool,
    pub app_up: bool,
    pub drained: bool,
    pub tags: Vec<String>,
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
    own: Arc<Mutex<Own>>,
    /// What this node signs every message with, and checks every message
    /// against. Here because nearly everything that sends or receives already
    /// has the node at hand.
    key: ClusterKey,
}

impl LocalNode {
    pub fn new(addr: SocketAddr, key: ClusterKey, tags: Vec<String>) -> Self {
        Self {
            id: NodeId::random(),
            addr,
            own: Arc::new(Mutex::new(Own {
                incarnation: Incarnation::ZERO,
                leaving: false,
                app_up: true,
                drained: false,
                tags,
            })),
            key,
        }
    }

    pub fn key(&self) -> &ClusterKey {
        &self.key
    }

    pub fn incarnation(&self) -> Incarnation {
        self.own.lock().unwrap().incarnation
    }

    pub fn is_ready(&self) -> bool {
        self.own.lock().unwrap().ready()
    }

    pub fn has_tag(&self, tag: &str) -> bool {
        self.own.lock().unwrap().tags.iter().any(|t| t == tag)
    }

    pub fn status(&self) -> Status {
        let own = self.own.lock().unwrap();
        Status {
            incarnation: own.incarnation,
            ready: own.ready(),
            app_up: own.app_up,
            drained: own.drained,
            tags: own.tags.clone(),
        }
    }

    /// How this node introduces itself to a peer.
    pub fn identity(&self) -> WireIdentity {
        let own = self.own.lock().unwrap();
        WireIdentity {
            id: self.id,
            addr: self.addr,
            incarnation: own.incarnation,
            ready: own.ready(),
            tags: own.tags.clone(),
        }
    }

    /// Answer a rumor that this node is suspect or dead: move our incarnation
    /// past it, so our "alive" outranks it everywhere. Returns the incarnation
    /// to advertise from now on, or `None` if this node is leaving and the
    /// rumor should stand.
    pub fn refute(&self, rumored: Incarnation) -> Option<Incarnation> {
        let mut own = self.own.lock().unwrap();
        if own.leaving {
            return None;
        }
        // a rumor older than our current incarnation is already beaten by it
        if rumored >= own.incarnation {
            own.incarnation = Incarnation::superseding(rumored);
        }
        Some(own.incarnation)
    }

    /// Stop defending this node's liveness. Its incarnation is final from
    /// here on, so a "dead" at that incarnation outranks every "alive" it
    /// ever sent.
    pub fn leave(&self) {
        // under the lock: a refute or change already past its check finishes
        // first, and the number we announce our death at can't move after this
        self.own.lock().unwrap().leaving = true;
    }

    /// Record whether this node's app is answering. True if that changed, in
    /// which case the caller should gossip `alive_rumor`.
    pub fn set_app_up(&self, app_up: bool) -> bool {
        self.change(|own| own.app_up = app_up)
    }

    /// Take this node out of rotation, or put it back. True if that changed.
    pub fn set_drained(&self, drained: bool) -> bool {
        self.change(|own| own.drained = drained)
    }

    /// Replace this node's tags. True if they changed. Check them with
    /// `wire::check_tags` first: every rumor about us carries them.
    pub fn set_tags(&self, tags: Vec<String>) -> bool {
        self.change(|own| own.tags = tags)
    }

    /// Apply `edit` to our own state, and if it changed anything, move the
    /// incarnation up one. True if it did, in which case the caller should
    /// gossip `alive_rumor`.
    ///
    /// The bump is what makes the change news: peers only take news about us
    /// at a higher incarnation than they hold, and only we can move ours, so
    /// this works the same way as a refute.
    fn change(&self, edit: impl FnOnce(&mut Own)) -> bool {
        let mut own = self.own.lock().unwrap();
        // leaving: the incarnation is final (see `leave`), and nobody is
        // sending us work any more anyway
        if own.leaving {
            return false;
        }
        let before = (own.app_up, own.drained, own.tags.clone());
        edit(&mut own);
        if (own.app_up, own.drained, own.tags.clone()) == before {
            return false;
        }
        own.incarnation = Incarnation::superseding(own.incarnation);
        true
    }

    /// "This node is alive", as a rumor to gossip.
    pub fn alive_rumor(&self) -> WireMember {
        let own = self.own.lock().unwrap();
        WireMember {
            id: self.id,
            addr: self.addr,
            incarnation: own.incarnation,
            state: WireMemberState::Alive,
            ready: own.ready(),
            tags: own.tags.clone(),
        }
    }
}
