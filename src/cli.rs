use clap::{Args, Parser, Subcommand};
use std::net::SocketAddr;

#[derive(Debug, Parser)]
#[command(
    name = "dalila",
    version,
    about = "Cluster membership and proxying agent"
)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the agent in the foreground.
    Start(StartArgs),

    /// Print the member table of a running agent.
    Members(MembersArgs),
}

#[derive(Debug, Args)]
pub struct StartArgs {
    #[arg(long, default_value = "0.0.0.0:7946")]
    pub bind: SocketAddr,

    /// The address peers use to reach this node. Needed when --bind is
    /// 0.0.0.0, which isn't an address anyone can connect to.
    #[arg(long, value_name = "HOST:PORT")]
    pub advertise: Option<SocketAddr>,

    #[arg(long, value_name = "HOST:PORT")]
    pub join: Vec<SocketAddr>,

    /// The secret every node in the cluster shares. Every message between
    /// nodes is signed with it, and a message without the right signature is
    /// dropped. Without it, anyone who can reach this node can join the
    /// cluster. Make one with `openssl rand -hex 32`, and prefer the env var:
    /// a flag shows up in `ps`.
    //
    // a plain string, checked later by `ClusterKey::from_secret`. if clap
    // checked it, its error would print the rejected secret
    #[arg(
        long,
        env = "DALILA_KEY",
        hide_env_values = true,
        value_name = "SECRET"
    )]
    pub key: Option<String>,

    /// What this node runs, like `web` or `api`. Repeat for more than one.
    /// Gossiped with the rest of its record, so every proxy in the cluster
    /// can pick members by tag. Up to 4, each up to 16 characters.
    #[arg(long = "tag", value_name = "TAG")]
    pub tags: Vec<String>,

    /// Accept client connections here and forward each to a live member.
    #[arg(long, value_name = "HOST:PORT", requires = "backend_port")]
    pub proxy: Option<SocketAddr>,

    /// Only forward to members with this tag. Without it, the proxy uses
    /// every member that can take work.
    #[arg(long, value_name = "TAG", requires = "proxy")]
    pub proxy_to: Option<String>,

    /// The port the app listens on, the same on every member. This node
    /// checks its own every second, and stops taking work while it doesn't
    /// answer. Without it, the node is never checked and always takes work.
    #[arg(long, value_name = "PORT")]
    pub backend_port: Option<u16>,

    /// Answer `GET /backends` here, with every member that can take work, for
    /// a proxy other than the built-in one. `--api` on its own means
    /// 127.0.0.1:7900. There's no auth, so keep it where only this machine
    /// can reach it.
    #[arg(
        long,
        value_name = "HOST:PORT",
        num_args = 0..=1,
        default_missing_value = "127.0.0.1:7900",
        requires = "backend_port"
    )]
    pub api: Option<SocketAddr>,
}

#[derive(Debug, Args)]
pub struct MembersArgs {
    #[arg(long, default_value = "127.0.0.1:7946")]
    pub addr: SocketAddr,

    /// The cluster's secret, the same one the node was started with.
    #[arg(
        long,
        env = "DALILA_KEY",
        hide_env_values = true,
        value_name = "SECRET"
    )]
    pub key: Option<String>,
}

pub fn parse() -> Cli {
    Cli::parse()
}
