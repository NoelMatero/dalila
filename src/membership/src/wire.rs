use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::node::{Incarnation, Member, MemberState, NodeId};

/// Bump when a change to the types below means two builds can no longer
/// understand each other.
///
/// 2: `WireIdentity` carries a whole address instead of just a port.
/// 3: `UdpBody::Leave`.
/// 4: members say whether their app is ready (`ready`).
pub const PROTOCOL_VERSION: u8 = 4;

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
        }
    }
}

/// A TCP frame's payload is `(PROTOCOL_VERSION, TcpBody)`. The version is the
/// first byte, so it can be checked before the body is decoded.
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
