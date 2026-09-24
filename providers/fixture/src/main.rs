use std::{io, process::ExitCode};

use tracing::{Level, error, info};
use tracing_subscriber::fmt::format::FmtSpan;
use trakkin_provider_fixture::FixtureAdapter;
use trakkin_provider_sdk::{read_launch_request, serve_adapter};

#[tokio::main]
async fn main() -> ExitCode {
    initialize_logging();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error!(error = ?error, "provider stopped with an error");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let launch = read_launch_request(io::stdin().lock())?;
    info!(
        process_instance_id = %launch.process_instance_id,
        "starting provider"
    );
    let adapter = FixtureAdapter::new(launch.process_instance_id.clone(), Default::default());
    let shutdown = adapter.shutdown_token().cancelled_owned();
    serve_adapter(&launch, adapter, io::stdout(), shutdown).await?;
    Ok(())
}

fn initialize_logging() {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(io::stderr)
        .init();
}
