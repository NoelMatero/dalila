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

    /// Accept client connections here and forward each to a live member.
    #[arg(long, value_name = "HOST:PORT", requires = "backend_port")]
    pub proxy: Option<SocketAddr>,

    /// The port every member serves the proxied service on.
    #[arg(long, value_name = "PORT", requires = "proxy")]
    pub backend_port: Option<u16>,
}

#[derive(Debug, Args)]
pub struct MembersArgs {
    #[arg(long, default_value = "127.0.0.1:7946")]
    pub addr: SocketAddr,
}

pub fn parse() -> Cli {
    Cli::parse()
}
