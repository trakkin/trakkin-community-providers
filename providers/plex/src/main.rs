use std::{io, process::ExitCode};

use tracing::{Level, error, info};
use tracing_subscriber::fmt::format::FmtSpan;
use trakkin_provider_plex::adapter::PlexAdapter;
use trakkin_provider_sdk::{read_launch_request, serve_adapter};

#[tokio::main]
async fn main() -> ExitCode {
    initialize_logging();
    match run().await {
        Ok(()) => {
            info!("provider stopped");
            ExitCode::SUCCESS
        }
        Err(_) => ExitCode::FAILURE,
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let launch = match read_launch_request(io::stdin().lock()) {
        Ok(launch) => launch,
        Err(error) => {
            error!(
                provider.stage = "bootstrap",
                error.message = %error,
                "provider failed to start"
            );
            return Err(error.into());
        }
    };
    info!(
        process_instance_id = %launch.process_instance_id,
        "starting provider"
    );
    let adapter = PlexAdapter::new(launch.process_instance_id.clone(), Default::default());
    let shutdown = adapter.shutdown_token().cancelled_owned();
    match serve_adapter(&launch, adapter, io::stdout(), shutdown).await {
        Ok(()) => Ok(()),
        Err(error) => {
            error!(
                provider.stage = "serve",
                error.message = %error,
                "provider stopped with an error"
            );
            Err(error.into())
        }
    }
}

fn initialize_logging() {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(io::stderr)
        .init();
}
