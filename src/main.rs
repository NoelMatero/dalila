use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // logs go to stderr, so `dalila members` output on stdout stays clean.
    // defaults to info; set RUST_LOG=debug (or any filter) to change it.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    dalila::run().await
}
