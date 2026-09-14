use crate::node::{Incarnation, Member, MemberState, NodeId};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WireIdentity {
    pub id: NodeId,
    pub port: u16,
    pub incarnation: Incarnation,
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

impl From<WireMemberState> for MemberState {
    fn from(member_state: WireMemberState) -> Self {
        match member_state {
            WireMemberState::Dead => MemberState::Dead,
            WireMemberState::Alive => MemberState::Alive,
            WireMemberState::Suspect => MemberState::Suspect { since: None },
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

// Keep the trait definition here so other modules can use it
pub trait FromTwo<A, B> {
    fn from_two(a: A, b: B) -> Self;
}
