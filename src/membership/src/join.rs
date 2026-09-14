use anyhow::{Context, bail};
use bytes::{Buf, BytesMut};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tracing::{info, warn};

use crate::node::LocalNode;
use crate::state::{MemberTable, MergeOutcome};
use crate::wire::{PROTOCOL_VERSION, TcpBody, WireIdentity, WireMember};

/// Largest payload we accept. Checked against the length prefix before we
/// wait for (or buffer towards) that many bytes.
const MAX_FRAME_LEN: usize = 4 * 1024 * 1024;

/// How long we wait on a peer to connect or reply.
const IO_TIMEOUT: Duration = Duration::from_secs(5);

pub struct TcpConnection {
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

    pub async fn connect(addr: SocketAddr) -> anyhow::Result<TcpConnection> {
        let stream = timeout(IO_TIMEOUT, TcpStream::connect(addr))
            .await
            .with_context(|| format!("timed out connecting to {addr}"))?
            .with_context(|| format!("could not connect to {addr}"))?;
        Ok(TcpConnection::new(stream))
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
        // version goes first, so the other side can check it before decoding the body
        let payload = postcard::to_allocvec(&(PROTOCOL_VERSION, body))?;

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

        // don't trust the peer's length: without this, a bogus prefix has us
        // buffering towards 4 GiB before we ever get to reject it
        if len > MAX_FRAME_LEN {
            bail!("peer announced a {len}-byte frame, limit is {MAX_FRAME_LEN}");
        }

        if self.buffer.len() < len + 4 {
            return Ok(None);
        }

        self.buffer.advance(4); // move buffer past the lenght prefix
        let payload = self.buffer.split_to(len); // get da payload via split_to

        // a peer on another version may lay the body out differently, so check
        // the version byte before decoding the rest
        match payload.first() {
            Some(&PROTOCOL_VERSION) => {}
            Some(other) => bail!("peer speaks protocol version {other}, we speak {PROTOCOL_VERSION}"),
            None => bail!("received an empty frame"),
        }

        let (_version, frame) = postcard::from_bytes::<(u8, TcpBody)>(&payload)?;

        Ok(Some(frame))
    }
}

// join the cluster through the first seed that answers
pub async fn join(
    addresses: Vec<SocketAddr>,
    local_node: &LocalNode,
    table: &MemberTable,
) -> anyhow::Result<()> {
    let body = TcpBody::JoinRequest {
        from: local_node.identity(),
    };

    for seed in &addresses {
        match join_through(*seed, &body, local_node, table).await {
            Ok(()) => {
                info!(%seed, members = table.len(), "joined cluster");
                return Ok(());
            }
            // a dead seed isn't fatal, try the next one
            Err(err) => warn!(%seed, "join failed: {err:#}"),
        }
    }

    bail!("none of the {} seed(s) accepted the join", addresses.len())
}

async fn join_through(
    seed: SocketAddr,
    body: &TcpBody,
    local_node: &LocalNode,
    table: &MemberTable,
) -> anyhow::Result<()> {
    let mut tcp_connection = TcpConnection::connect(seed).await?;
    tcp_connection.write_frame(body).await?;

    // the seed answers on this same connection
    let reply = timeout(IO_TIMEOUT, tcp_connection.read_frame())
        .await
        .context("timed out waiting for the join response")??;

    match reply {
        Some(TcpBody::JoinResponse { from, members }) => {
            handle_join_response(local_node, table, from, seed, members);
            Ok(())
        }
        Some(_) => bail!("seed replied with something other than a join response"),
        None => bail!("seed closed the connection without replying"),
    }
}

// ask a running node for its member table
pub async fn fetch_members(
    addr: SocketAddr,
) -> anyhow::Result<(WireIdentity, Vec<WireMember>)> {
    let mut tcp_connection = TcpConnection::connect(addr).await?;
    tcp_connection.write_frame(&TcpBody::MembersRequest).await?;

    let reply = timeout(IO_TIMEOUT, tcp_connection.read_frame())
        .await
        .context("timed out waiting for the member list")??;

    match reply {
        Some(TcpBody::MembersResponse { from, members }) => Ok((from, members)),
        Some(_) => bail!("node replied with something other than a member list"),
        None => bail!("node closed the connection without replying"),
    }
}

pub async fn start_tcp_accept_loop(listener: TcpListener, node: LocalNode, table: MemberTable) {
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                // one bad accept shouldn't stop the node accepting anyone else.
                // the pause is for errors that don't clear by themselves (out of
                // file descriptors), which would otherwise spin this loop
                warn!("accept failed: {err}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let table = table.clone();
        let tmp_node = node.clone();

        tokio::spawn(async move {
            let mut tcp_connection = TcpConnection::new(stream);

            // an error ends this connection only, never the accept loop
            if let Err(err) = handle_connection(&mut tcp_connection, &tmp_node, &table, addr).await
            {
                warn!(%addr, "dropped connection: {err:#}");
            }
        });
    }
}

async fn handle_connection(
    tcp_connection: &mut TcpConnection,
    node: &LocalNode,
    table: &MemberTable,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    // one reply per request, until the peer hangs up (Ok(None))
    while let Some(frame) = tcp_connection.read_frame().await? {
        let reply = match frame {
            TcpBody::JoinRequest { from } => handle_join_request(node, table, from, addr),
            TcpBody::MembersRequest => TcpBody::MembersResponse {
                from: node.identity(),
                members: table.snapshot().into_iter().map(Into::into).collect(),
            },
            TcpBody::JoinResponse { .. } | TcpBody::MembersResponse { .. } => {
                bail!("got a response where a request was expected")
            }
        };

        tcp_connection.write_frame(&reply).await?;
    }

    Ok(())
}

pub fn handle_join_request(
    our_node: &LocalNode,
    table: &MemberTable,
    recvd_wire_identity: WireIdentity,
    recvd_addr: SocketAddr,
) -> TcpBody {
    // snapshot before adding the joiner, so it isn't told about itself
    let members = table.snapshot().into_iter().map(Into::into).collect();

    apply(table, our_node, recvd_wire_identity.observed_at(recvd_addr));

    TcpBody::JoinResponse {
        from: our_node.identity(),
        members,
    }
}

pub fn handle_join_response(
    our_node: &LocalNode,
    table: &MemberTable,
    recvd_wire_identity: WireIdentity,
    recvd_addr: SocketAddr,
    recvd_members: Vec<WireMember>,
) {
    // the seed has no entry for itself in its own table, so build its record
    // the same way it built ours: ip from the connection, port from the identity
    apply(table, our_node, recvd_wire_identity.observed_at(recvd_addr));

    for member in recvd_members {
        apply(table, our_node, member);
    }
}

// merge one rumor into the table and log what happened
fn apply(table: &MemberTable, our_node: &LocalNode, rumor: WireMember) {
    let (id, addr) = (rumor.id, rumor.addr);

    match table.merge(rumor, our_node.id) {
        MergeOutcome::Added => info!(%id, %addr, "new member"),
        MergeOutcome::Updated | MergeOutcome::Ignored => {}
        MergeOutcome::Refute { rumored } => {
            // can't happen through join: every process starts with a fresh id, so
            // nobody has had time to suspect it yet. becomes reachable once
            // rumors spread by gossip, which is where the incarnation bump belongs
            warn!(%rumored, "a peer believes this node is not alive; refutation not implemented yet");
        }
    }
}
