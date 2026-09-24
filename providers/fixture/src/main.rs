use std::{io, process::ExitCode};

use trakkin_provider_fixture::FixtureAdapter;
use trakkin_provider_sdk::{read_launch_request, serve_adapter};

#[tokio::main]
async fn main() -> ExitCode {
    if run().await.is_ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let launch = read_launch_request(io::stdin().lock())?;
    let adapter = FixtureAdapter::new(launch.process_instance_id.clone(), Default::default());
    let shutdown = adapter.shutdown_token().cancelled_owned();
    serve_adapter(&launch, adapter, io::stdout(), shutdown).await?;
    Ok(())
}
