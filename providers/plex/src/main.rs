use std::{io, process::ExitCode};

use trakkin_provider_plex::{ADAPTER_KEY, adapter::PlexAdapter};
use trakkin_provider_sdk::{init_provider_tracing, read_launch_request, serve_adapter};

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(error) = init_provider_tracing(ADAPTER_KEY) {
        eprintln!("provider tracing initialization failed: {error}");
        return ExitCode::FAILURE;
    }
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(event = "provider.failed", error = %error);
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let launch = read_launch_request(io::stdin().lock())?;
    tracing::info!(
        event = "provider.starting",
        provider.id = ADAPTER_KEY,
        process.instance_id = %launch.process_instance_id,
    );
    let adapter = PlexAdapter::new(launch.process_instance_id.clone(), Default::default());
    let shutdown = adapter.shutdown_token().cancelled_owned();
    serve_adapter(&launch, adapter, io::stdout(), shutdown).await?;
    tracing::info!(
        event = "provider.stopped",
        provider.id = ADAPTER_KEY,
        process.instance_id = %launch.process_instance_id,
    );
    Ok(())
}
