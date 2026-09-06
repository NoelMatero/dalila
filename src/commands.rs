pub mod members;
pub mod start;

use anyhow::Result;

pub async fn dispatch(cmd: crate::cli::Command) -> Result<()> {
    match cmd {
        crate::cli::Command::Start(args) => start::execute(args.into()).await,
        crate::cli::Command::Members(args) => members::execute(args.into()).await,
    }
}
