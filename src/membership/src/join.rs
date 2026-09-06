use anyhow::Ok;
use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

use crate::node::{LocalNode, Member};
use crate::wire::{WireIdentity, WireMember};

#[derive(Serialize, Deserialize, Debug)]
enum TcpBody {
    JoinRequest {
        from: WireIdentity,
    },
    JoinResponse {
        from: WireIdentity,
        members: Vec<WireMember>,
    },
}

impl TcpBody {
    pub fn join_request(local_node: LocalNode) -> Self {
        TcpBody::JoinRequest {
            from: WireIdentity::new(local_node),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Envelope {
    pub version: u8,
    pub body: TcpBody,
}

struct TcpConnection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl TcpConnection {
    pub fn new(stream: TcpStream) -> TcpConnection {
        TcpConnection {
            stream,
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub async fn read_frame(&mut self) -> anyhow::Result<Option<TcpBody>> {
        loop {
            if let Some(frame) = self.parse_frame().await? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    anyhow::bail!("connection closed with incomplete frame");
                }
            }
        }
    }

    pub async fn write_frame(&mut self, body: &TcpBody) -> anyhow::Result<()> {
        let payload = postcard::to_allocvec(body)?;

        let len = u32::try_from(payload.len())?;

        self.stream.write_u32(len).await?;
        self.stream.write_all(&payload).await?;

        Ok(())
    }

    pub async fn parse_frame(&mut self) -> anyhow::Result<Option<TcpBody>> {
        if self.buffer.len() < 4 {
            return Ok(None);
        }

        let len = u32::from_be_bytes([
            self.buffer[0],
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
        ]) as usize;

        if self.buffer.len() < len + 4 {
            return Ok(None);
        }

        self.buffer.advance(4); // move buffer past the lenght prefix
        let payload = self.buffer.split_to(len); // get da payload via split_to

        let frame = postcard::from_bytes::<TcpBody>(&payload)?;

        Ok(Some(frame))
    }
}

// join other nodes starting with a node at addr: join_addr
pub async fn join(addresses: Vec<SocketAddr>, local_node: LocalNode) -> anyhow::Result<()> {
    let body = TcpBody::join_request(local_node);

    for socket in addresses {
        let stream = TcpStream::connect(socket).await?;
        let mut tcp_connection = TcpConnection::new(stream);
        tcp_connection.write_frame(&body).await?;
    }

    Ok(())

    /*
    send join tcp request
    clien processes this, understands it
    after this, client sends payload: member list
    then client closes the connection
    */
}

pub async fn start_tcp_accept_loop(
    addr: SocketAddr,
    members: Vec<Member>,
) -> anyhow::Result<(), Box<dyn std::error::Error + 'static>> {
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, addr) = listener.accept().await?;

        let wire_members: Vec<WireMember> =
            members.clone().into_iter().map(WireMember::from).collect();
        tokio::spawn(async move {
            let mut tcp_connection = TcpConnection::new(stream);

            loop {
                if let Some(frame) = tcp_connection.read_frame().await.unwrap() {
                    match frame {
                        TcpBody::JoinRequest { from } => {
                            handle_join_request(from, addr, wire_members.clone()).await
                        }
                        TcpBody::JoinResponse { from, members } => {
                            handle_join_response(from, addr, members).await
                        }
                    }
                }
            }
        });
    }
}

pub async fn handle_join_request(
    recv_wire_identity: WireIdentity,
    recv_addr: SocketAddr,
    mut members: Vec<WireMember>,
) {
    /*

        return all of the members:

    */
}

pub async fn handle_join_response(
    wire_identity: WireIdentity,
    addr: SocketAddr,
    members: Vec<WireMember>,
) {
    println!(
        "todo: join response  data: {:?}, {:?}, {:?}",
        wire_identity, addr, members
    );
}
