use std::net::SocketAddr;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::node::{Incarnation, Member, MemberState, NodeId};

/// Bump when a change to the types below means two builds can no longer
/// understand each other.
///
/// 2: `WireIdentity` carries a whole address instead of just a port.
/// 3: `UdpBody::Leave`.
/// 4: members say whether their app is ready (`ready`).
/// 5: every message ends with a tag made from the cluster key (`auth`).
/// 6: members say what they run (`tags`).
pub const PROTOCOL_VERSION: u8 = 6;

/// Most tags one node can have, and the longest one can be.
///
/// Small because every rumor carries its member's tags, and up to ten rumors
/// share a datagram that must stay under 1400 bytes. At these limits a rumor
/// is at most 113 bytes and the fullest datagram about 1300. At 8 tags of 32
/// it would be 3500, and gossip would stop working.
pub const MAX_TAGS: usize = 4;
pub const MAX_TAG_LEN: usize = 16;

/// Reject tags that break the limits above, or that hold anything besides
/// letters, digits, `-`, `_` and `.`. Checked where tags are given, at
/// startup and in the control API: a peer's tags are trusted like the rest
/// of what it says.
pub fn check_tags(tags: &[String]) -> Result<()> {
    if tags.len() > MAX_TAGS {
        bail!(
            "{} tags given, a node can have at most {MAX_TAGS}",
            tags.len()
        );
    }
    for tag in tags {
        let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.');
        if tag.is_empty() || tag.len() > MAX_TAG_LEN || !tag.chars().all(allowed) {
            bail!("tag {tag:?}: needs 1 to {MAX_TAG_LEN} letters, digits, '-', '_' or '.'");
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WireIdentity {
    pub id: NodeId,
    /// Where this node can be reached, as the node itself states it.
    ///
    /// Stated rather than read off the connection: the IP a connection comes
    /// from is whichever one the OS picked for that destination, and on a
    /// machine with more than one address that need not be the one the node
    /// listens on.
    pub addr: SocketAddr,
    pub incarnation: Incarnation,
    pub ready: bool,
    pub tags: Vec<String>,
}

impl WireIdentity {
    /// The member record for the node introducing itself.
    pub fn into_member(self) -> WireMember {
        WireMember {
            id: self.id,
            addr: self.addr,
            incarnation: self.incarnation,
            state: WireMemberState::Alive,
            ready: self.ready,
            tags: self.tags,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum WireMemberState {
    Alive,
    Suspect,
    Dead,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WireMember {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub incarnation: Incarnation,
    pub state: WireMemberState,
    pub ready: bool,
    pub tags: Vec<String>,
}

impl From<MemberState> for WireMemberState {
    fn from(member_state: MemberState) -> Self {
        match member_state {
            MemberState::Dead { since: _ } => WireMemberState::Dead,
            MemberState::Alive => WireMemberState::Alive,
            MemberState::Suspect { since: _ } => WireMemberState::Suspect,
        }
    }
}

impl WireMember {
    pub fn new(member: Member) -> WireMember {
        WireMember {
            id: member.id,
            addr: member.addr,
            incarnation: member.incarnation,
            state: WireMemberState::from(member.state),
            ready: member.ready,
            tags: member.tags,
        }
    }
}

impl From<Member> for WireMember {
    fn from(member: Member) -> Self {
        WireMember::new(member)
    }
}

/// Converting in is the moment this node learns of a suspicion or a death, so
/// that is when its own clock for it starts.
impl From<WireMemberState> for MemberState {
    fn from(member_state: WireMemberState) -> Self {
        match member_state {
            WireMemberState::Dead => MemberState::Dead {
                since: Instant::now(),
            },
            WireMemberState::Alive => MemberState::Alive,
            WireMemberState::Suspect => MemberState::Suspect {
                since: Instant::now(),
            },
        }
    }
}

impl From<WireMember> for Member {
    fn from(member: WireMember) -> Self {
        Member {
            id: member.id,
            addr: member.addr,
            incarnation: member.incarnation,
            state: MemberState::from(member.state),
            ready: member.ready,
            tags: member.tags,
        }
    }
}

/// A TCP frame's payload is `(PROTOCOL_VERSION, TcpBody)` followed by its tag.
/// The version is the first byte, so it can be checked before anything else;
/// the tag covers the version and the body both.
#[derive(Serialize, Deserialize, Debug)]
pub enum TcpBody {
    JoinRequest {
        from: WireIdentity,
    },
    JoinResponse {
        from: WireIdentity,
        members: Vec<WireMember>,
    },
    MembersRequest,
    MembersResponse {
        from: WireIdentity,
        members: Vec<WireMember>,
    },

    /// "Here is everything I believe. Send me everything you believe."
    ///
    /// Over TCP rather than UDP because a whole table outgrows a datagram:
    /// a rumor is 30-45 bytes, so a hundred members is already past the 1400
    /// we allow ourselves there.
    SyncRequest {
        from: WireIdentity,
        members: Vec<WireMember>,
    },
    /// The other half of the exchange. Sent before the request's rumors are
    /// merged, so it doesn't echo back what it was just told.
    SyncResponse {
        from: WireIdentity,
        members: Vec<WireMember>,
    },
}
