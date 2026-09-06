use crate::node::{Incarnation, LocalNode, Member, MemberState, NodeId};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Debug)]
pub struct WireIdentity {
    pub id: NodeId,
    pub port: u16,
    pub incarnation: Incarnation,
}

impl WireIdentity {
    pub fn new(local_node: LocalNode) -> WireIdentity {
        WireIdentity {
            id: local_node.id,
            port: local_node.bind.port(),
            incarnation: local_node.incarnation,
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
