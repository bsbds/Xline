use anyhow::Result;
use benchmark::{Benchmark, CommandRunner};
use clap::Parser;
use tracing::info;

fn tracing_init(_stdout: bool) {
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Benchmark = Benchmark::parse();
    tracing_init(args.stdout);

    let mut runner = CommandRunner::new(args);
    let stats = runner.run().await?;

    let summary = stats.summary();
    let histogram = stats.histogram();

    info!("{}", summary);
    info!("{}", histogram);

    Ok(())
}
