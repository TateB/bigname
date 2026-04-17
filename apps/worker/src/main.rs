use anyhow::{Context, Result};
use bigname_storage::DatabaseConfig;
use clap::{Args, Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "bigname-worker",
    about = "Bootstrap worker process for bigname"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Run(RunArgs),
    Migrate(MigrateArgs),
}

#[derive(Args, Debug)]
struct RunArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_WORKER_POLL_INTERVAL_SECS",
        default_value_t = 5_u64
    )]
    poll_interval_secs: u64,
}

#[derive(Args, Debug)]
struct MigrateArgs {
    #[command(flatten)]
    database: DatabaseConfig,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing("bigname-worker");

    match Cli::parse().command {
        Command::Run(args) => run(args).await,
        Command::Migrate(args) => migrate(args).await,
    }
}

async fn run(args: RunArgs) -> Result<()> {
    let _pool = bigname_storage::connect(&args.database).await?;

    info!(
        service = "worker",
        phase = bigname_domain::bootstrap_phase(),
        execution_status = bigname_execution::bootstrap_status(),
        poll_interval_secs = args.poll_interval_secs,
        "worker booted"
    );

    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for shutdown signal")?;
    info!(service = "worker", "shutdown signal received");
    Ok(())
}

async fn migrate(args: MigrateArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    bigname_storage::migrate(&pool).await?;
    info!(service = "worker", "database migrations applied");
    Ok(())
}

fn init_tracing(service: &'static str) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if std::env::var_os("BIGNAME_LOG_JSON").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_target(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .with_target(false)
            .init();
    }

    info!(
        service = service,
        phase = bigname_domain::bootstrap_phase(),
        "logging configured"
    );
}
