use std::net::SocketAddr;

use anyhow::Result;
use membership::join::fetch_members;
use membership::wire::WireMemberState;

pub struct Config {
    pub addr: SocketAddr,
}

impl From<crate::cli::MembersArgs> for Config {
    fn from(args: crate::cli::MembersArgs) -> Self {
        Self { addr: args.addr }
    }
}

pub async fn execute(cfg: Config) -> Result<()> {
    let (from, mut members) = fetch_members(cfg.addr).await?;
    members.sort_by_key(|m| m.addr);

    print_row("ID", "ADDRESS", "INCARNATION", "STATE");
    // the node we asked isn't in its own table, so it gets a row of its own
    print_row(
        &from.id.to_string(),
        &cfg.addr.to_string(),
        &from.incarnation.to_string(),
        "alive (queried)",
    );
    for m in members {
        print_row(
            &m.id.to_string(),
            &m.addr.to_string(),
            &m.incarnation.to_string(),
            state_label(m.state),
        );
    }

    Ok(())
}

fn print_row(id: &str, addr: &str, incarnation: &str, state: &str) {
    println!("{id:<36}  {addr:<21}  {incarnation:>11}  {state}");
}

fn state_label(state: WireMemberState) -> &'static str {
    match state {
        WireMemberState::Alive => "alive",
        WireMemberState::Suspect => "suspect",
        WireMemberState::Dead => "dead",
    }
}
