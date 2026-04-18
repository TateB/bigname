mod address_names;
mod children;
mod name_current;

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
    AddressNamesCurrent(AddressNamesCurrentArgs),
    ChildrenCurrent(ChildrenCurrentArgs),
    NameCurrent(NameCurrentArgs),
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

#[derive(Args, Debug)]
struct NameCurrentArgs {
    #[command(subcommand)]
    command: NameCurrentCommand,
}

#[derive(Args, Debug)]
struct AddressNamesCurrentArgs {
    #[command(subcommand)]
    command: AddressNamesCurrentCommand,
}

#[derive(Args, Debug)]
struct ChildrenCurrentArgs {
    #[command(subcommand)]
    command: ChildrenCurrentCommand,
}

#[derive(Subcommand, Debug)]
enum NameCurrentCommand {
    Rebuild(NameCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
enum AddressNamesCurrentCommand {
    Rebuild(AddressNamesCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
enum ChildrenCurrentCommand {
    Rebuild(ChildrenCurrentRebuildArgs),
}

#[derive(Args, Debug)]
struct NameCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    logical_name_id: Option<String>,
}

#[derive(Args, Debug)]
struct AddressNamesCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    address: Option<String>,
}

#[derive(Args, Debug)]
struct ChildrenCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    logical_name_id: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing("bigname-worker");

    match Cli::parse().command {
        Command::Run(args) => run(args).await,
        Command::Migrate(args) => migrate(args).await,
        Command::AddressNamesCurrent(args) => address_names_current(args).await,
        Command::ChildrenCurrent(args) => children_current(args).await,
        Command::NameCurrent(args) => name_current(args).await,
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

async fn name_current(args: NameCurrentArgs) -> Result<()> {
    match args.command {
        NameCurrentCommand::Rebuild(args) => rebuild_name_current(args).await,
    }
}

async fn address_names_current(args: AddressNamesCurrentArgs) -> Result<()> {
    match args.command {
        AddressNamesCurrentCommand::Rebuild(args) => rebuild_address_names_current(args).await,
    }
}

async fn children_current(args: ChildrenCurrentArgs) -> Result<()> {
    match args.command {
        ChildrenCurrentCommand::Rebuild(args) => rebuild_children_current(args).await,
    }
}

async fn rebuild_name_current(args: NameCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary =
        name_current::rebuild_name_current(&pool, args.logical_name_id.as_deref()).await?;

    info!(
        service = "worker",
        projection = "name_current",
        requested_name_count = summary.requested_name_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        logical_name_id = args.logical_name_id.as_deref().unwrap_or("all"),
        "name_current rebuild completed"
    );

    Ok(())
}

async fn rebuild_address_names_current(args: AddressNamesCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary =
        address_names::rebuild_address_names_current(&pool, args.address.as_deref()).await?;

    info!(
        service = "worker",
        projection = "address_names_current",
        requested_address_count = summary.requested_address_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        address = args.address.as_deref().unwrap_or("all"),
        "address_names_current rebuild completed"
    );

    Ok(())
}

async fn rebuild_children_current(args: ChildrenCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary =
        children::rebuild_children_current(&pool, args.logical_name_id.as_deref()).await?;

    info!(
        service = "worker",
        projection = "children_current",
        requested_parent_count = summary.requested_parent_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        logical_name_id = args.logical_name_id.as_deref().unwrap_or("all"),
        "children_current rebuild completed"
    );

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
