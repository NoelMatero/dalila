pub mod cli;
pub mod commands;

use anyhow::Result;

pub async fn run() -> Result<()> {
    let cli = cli::parse();
    commands::dispatch(cli.command).await
}
