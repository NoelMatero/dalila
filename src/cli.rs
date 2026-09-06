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

    #[arg(long, value_name = "HOST:PORT")]
    pub join: Vec<SocketAddr>,
}

#[derive(Debug, Args)]
pub struct MembersArgs {
    #[arg(long, default_value = "127.0.0.1:7946")]
    pub addr: SocketAddr,
}

pub fn parse() -> Cli {
    Cli::parse()
}
