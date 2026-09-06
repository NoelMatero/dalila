use crate::node::{Incarnation, LocalNode, NodeId};
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

#[derive(Serialize, Deserialize, Debug)]
pub struct WireMember {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub incarnation: Incarnation,
    pub state: WireMemberState,
}
