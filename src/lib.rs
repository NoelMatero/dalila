pub mod cli;
pub mod commands;
pub mod health;
pub mod proxy;

use anyhow::Result;

pub async fn run() -> Result<()> {
    let cli = cli::parse();
    commands::dispatch(cli.command).await
}
