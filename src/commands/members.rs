use std::net::SocketAddr;

use anyhow::Result;
use membership::auth::ClusterKey;
use membership::join::fetch_members;
use membership::wire::WireMemberState;

pub struct Config {
    pub addr: SocketAddr,
    pub key: Option<String>,
}

impl From<crate::cli::MembersArgs> for Config {
    fn from(args: crate::cli::MembersArgs) -> Self {
        Self {
            addr: args.addr,
            key: args.key,
        }
    }
}

pub async fn execute(cfg: Config) -> Result<()> {
    // no key: only a node started without one will answer
    let key = match cfg.key {
        Some(secret) => ClusterKey::from_secret(&secret)?,
        None => ClusterKey::none(),
    };
    let (from, mut members) = fetch_members(cfg.addr, key).await?;
    members.sort_by_key(|m| m.addr);

    print_row("ID", "ADDRESS", "INCARNATION", "TAGS", "STATE");
    // the node we asked isn't in its own table, so it gets a row of its own
    print_row(
        &from.id.to_string(),
        &from.addr.to_string(),
        &from.incarnation.to_string(),
        &tags_label(&from.tags),
        &format!("{} (queried)", state_label(WireMemberState::Alive, from.ready)),
    );
    for m in members {
        print_row(
            &m.id.to_string(),
            &m.addr.to_string(),
            &m.incarnation.to_string(),
            &tags_label(&m.tags),
            &state_label(m.state, m.ready),
        );
    }

    Ok(())
}

fn print_row(id: &str, addr: &str, incarnation: &str, tags: &str, state: &str) {
    // 23 fits two tags of the most common length. more just pushes STATE right
    println!("{id:<36}  {addr:<21}  {incarnation:>11}  {tags:<23}  {state}");
}

fn tags_label(tags: &[String]) -> String {
    if tags.is_empty() {
        "-".to_string()
    } else {
        tags.join(",")
    }
}

fn state_label(state: WireMemberState, ready: bool) -> String {
    let state = match state {
        WireMemberState::Alive => "alive",
        WireMemberState::Suspect => "suspect",
        WireMemberState::Dead => "dead",
    };
    // only worth saying when it's news: nearly every member is ready. not
    // ready is its app being down, or it being drained; its own `GET /node`
    // says which
    if ready {
        state.to_string()
    } else {
        format!("{state}, not ready")
    }
}
