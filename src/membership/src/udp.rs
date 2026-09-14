use std::net::SocketAddr;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

use crate::{
    node::{LocalNode, Member},
    state::MemberTable,
    wire::{WireIdentity, WireMember},
};

#[derive(Serialize, Deserialize, Debug, Clone)]
enum UdpBody {
    Ping {
        from: WireIdentity,
        their_members: Vec<WireMember>,
    },

    Ack {
        from: WireIdentity,
        new_members: Option<WireMember>,
    },
}

impl UdpBody {
    fn encode_frame(self) -> anyhow::Result<Vec<u8>> {
        let payload = postcard::to_allocvec(&self)?;
        Ok(payload)
    }
}

fn decode_frame(payload: &[u8]) -> Result<UdpBody> {
    let frame = postcard::from_bytes::<UdpBody>(payload)?;
    Ok(frame)
}

pub trait IntoTokioSocket {
    async fn into_socket(self) -> Result<UdpSocket>;
}

impl IntoTokioSocket for SocketAddr {
    async fn into_socket(self) -> Result<UdpSocket> {
        UdpSocket::bind(self)
            .await
            .with_context(|| format!("Failed to bind UDP socket to address: {}", self))
    }
}

impl IntoTokioSocket for UdpSocket {
    async fn into_socket(self) -> anyhow::Result<UdpSocket> {
        Ok(self)
    }
}

async fn handle_ping_req(
    socket: &UdpSocket,
    from: WireIdentity,
    their_members: Vec<WireMember>,
    table: MemberTable,
) -> anyhow::Result<()> {
    let _their_members: Vec<Member> = their_members.into_iter().map(Into::into).collect();
    let _ours = table.snapshot();
    // compare these guys, perhaps report the result, then update if needed

    todo!()

    // send them an ack back and then new members, if such exist
    // incarnation?
}

async fn handle_ack_req(socket: &UdpSocket, from: WireIdentity, their_members: Vec<WireMember>) {
    // resolve the suspect thing we added after pinging
}

async fn send_udp<T: IntoTokioSocket>(target: T, body: UdpBody) -> Result<()> {
    let socket = target.into_socket().await?;

    match body {
        UdpBody::Ping {
            from,
            their_members,
        } => {}
        UdpBody::Ack { from, new_members } => {}
    }

    todo!()
}

pub async fn start_udp_loop(node: LocalNode, table: MemberTable) -> anyhow::Result<()> {
    let socket = UdpSocket::bind(node.bind).await?;

    let mut buf = [0u8; 4096];

    loop {
        let (len, addr) = socket.recv_from(&mut buf).await?;

        let frame = decode_frame(&buf)?;

        match frame {
            UdpBody::Ping {
                from,
                their_members,
            } => {
                handle_ping_req(&socket, from, their_members, table.clone()).await?;
            }
            UdpBody::Ack { from, new_members } => {}
        }
    }
    todo!()
}
