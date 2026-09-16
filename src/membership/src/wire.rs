use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::node::{Incarnation, Member, MemberState, NodeId};

/// Bump when a change to the types below means two builds can no longer
/// understand each other.
pub const PROTOCOL_VERSION: u8 = 1;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WireIdentity {
    pub id: NodeId,
    pub port: u16,
    pub incarnation: Incarnation,
}

impl WireIdentity {
    /// The member record for a peer we are talking to at `peer`.
    ///
    /// The IP comes from the connection, since that is how the two of us
    /// actually reach each other. The port comes from the identity: a
    /// connection's source port is an ephemeral one nobody is listening on.
    pub fn observed_at(self, peer: SocketAddr) -> WireMember {
        WireMember {
            id: self.id,
            addr: SocketAddr::new(peer.ip(), self.port),
            incarnation: self.incarnation,
            state: WireMemberState::Alive,
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
}

impl From<MemberState> for WireMemberState {
    fn from(member_state: MemberState) -> Self {
        match member_state {
            MemberState::Dead => WireMemberState::Dead,
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
        }
    }
}

impl From<Member> for WireMember {
    fn from(member: Member) -> Self {
        WireMember::new(member)
    }
}

/// Converting in is the moment this node learns of a suspicion, so that is
/// when its own clock for it starts.
impl From<WireMemberState> for MemberState {
    fn from(member_state: WireMemberState) -> Self {
        match member_state {
            WireMemberState::Dead => MemberState::Dead,
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
}
