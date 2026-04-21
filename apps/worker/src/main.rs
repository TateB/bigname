mod address_names;
mod children;
mod execution;
mod inspect;
mod name_current;
mod permissions;
mod primary_name;
mod record_inventory;
mod replay;
mod resolver;

use anyhow::{Context, Result};
use bigname_storage::DatabaseConfig;
use clap::{Args, Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

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
    Execution(ExecutionArgs),
    Inspect(inspect::InspectArgs),
    NameCurrent(NameCurrentArgs),
    PermissionsCurrent(PermissionsCurrentArgs),
    PrimaryNamesCurrent(PrimaryNamesCurrentArgs),
    Replay(ReplayArgs),
    RecordInventoryCurrent(RecordInventoryCurrentArgs),
    ResolverCurrent(ResolverCurrentArgs),
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

#[derive(Args, Debug)]
struct ExecutionArgs {
    #[command(subcommand)]
    command: ExecutionCommand,
}

#[derive(Args, Debug)]
struct PermissionsCurrentArgs {
    #[command(subcommand)]
    command: PermissionsCurrentCommand,
}

#[derive(Args, Debug)]
struct PrimaryNamesCurrentArgs {
    #[command(subcommand)]
    command: PrimaryNamesCurrentCommand,
}

#[derive(Args, Debug)]
struct ReplayArgs {
    #[command(subcommand)]
    command: ReplayCommand,
}

#[derive(Args, Debug)]
struct RecordInventoryCurrentArgs {
    #[command(subcommand)]
    command: RecordInventoryCurrentCommand,
}

#[derive(Args, Debug)]
struct ResolverCurrentArgs {
    #[command(subcommand)]
    command: ResolverCurrentCommand,
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

#[derive(Subcommand, Debug)]
enum ExecutionCommand {
    InvalidateVerifiedResolutionManifest(InvalidateVerifiedResolutionManifestArgs),
    InvalidateVerifiedResolutionTopologyBoundary(InvalidateVerifiedResolutionBoundaryArgs),
    InvalidateVerifiedResolutionRecordBoundary(InvalidateVerifiedResolutionBoundaryArgs),
    InvalidateVerifiedPrimaryNameManifest(InvalidateVerifiedPrimaryNameManifestArgs),
    InvalidateVerifiedPrimaryNameTopologyBoundary(InvalidateVerifiedPrimaryNameBoundaryArgs),
    InvalidateVerifiedPrimaryNameRecordBoundary(InvalidateVerifiedPrimaryNameBoundaryArgs),
}

#[derive(Subcommand, Debug)]
enum PermissionsCurrentCommand {
    Rebuild(PermissionsCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
enum PrimaryNamesCurrentCommand {
    Rebuild(PrimaryNamesCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
enum ReplayCommand {
    AllCurrentProjections(AllCurrentProjectionsArgs),
}

#[derive(Subcommand, Debug)]
enum RecordInventoryCurrentCommand {
    Rebuild(RecordInventoryCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
enum ResolverCurrentCommand {
    Rebuild(ResolverCurrentRebuildArgs),
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

#[derive(Args, Debug)]
struct InvalidateVerifiedResolutionManifestArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    source_manifest_id: Option<i64>,
    #[arg(long)]
    source_family: Option<String>,
    #[arg(long)]
    manifest_version: i64,
}

#[derive(Args, Debug)]
struct InvalidateVerifiedResolutionBoundaryArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    logical_name_id: String,
    #[arg(long)]
    resource_id: Uuid,
    #[arg(long)]
    normalized_event_id: Option<i64>,
    #[arg(long)]
    event_kind: Option<String>,
    #[arg(long)]
    chain_id: String,
    #[arg(long)]
    block_number: i64,
    #[arg(long)]
    block_hash: String,
    #[arg(long)]
    timestamp: String,
}

#[derive(Args, Debug)]
struct InvalidateVerifiedPrimaryNameManifestArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    address: String,
    #[arg(long)]
    coin_type: String,
    #[arg(long)]
    source_manifest_id: Option<i64>,
    #[arg(long)]
    source_family: Option<String>,
    #[arg(long)]
    manifest_version: i64,
}

#[derive(Args, Debug)]
struct InvalidateVerifiedPrimaryNameBoundaryArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    address: String,
    #[arg(long)]
    coin_type: String,
    #[arg(long)]
    logical_name_id: String,
    #[arg(long)]
    resource_id: Uuid,
    #[arg(long)]
    normalized_event_id: Option<i64>,
    #[arg(long)]
    event_kind: Option<String>,
    #[arg(long)]
    chain_id: String,
    #[arg(long)]
    block_number: i64,
    #[arg(long)]
    block_hash: String,
    #[arg(long)]
    timestamp: String,
}

#[derive(Args, Debug)]
struct PermissionsCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    resource_id: Option<String>,
}

#[derive(Args, Debug)]
struct PrimaryNamesCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    address: Option<String>,
    #[arg(long)]
    namespace: Option<String>,
    #[arg(long)]
    coin_type: Option<String>,
}

#[derive(Args, Debug)]
struct AllCurrentProjectionsArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct RecordInventoryCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    resource_id: Option<String>,
}

#[derive(Args, Debug)]
struct ResolverCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    chain_id: Option<String>,
    #[arg(long)]
    resolver_address: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing("bigname-worker", cli.writes_machine_json());

    match cli.command {
        Command::Run(args) => run(args).await,
        Command::Migrate(args) => migrate(args).await,
        Command::AddressNamesCurrent(args) => address_names_current(args).await,
        Command::ChildrenCurrent(args) => children_current(args).await,
        Command::Execution(args) => execution_command(args).await,
        Command::Inspect(args) => inspect::inspect_command(args).await,
        Command::NameCurrent(args) => name_current(args).await,
        Command::PermissionsCurrent(args) => permissions_current(args).await,
        Command::PrimaryNamesCurrent(args) => primary_names_current(args).await,
        Command::Replay(args) => replay_command(args).await,
        Command::RecordInventoryCurrent(args) => record_inventory_current(args).await,
        Command::ResolverCurrent(args) => resolver_current(args).await,
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

async fn execution_command(args: ExecutionArgs) -> Result<()> {
    match args.command {
        ExecutionCommand::InvalidateVerifiedResolutionManifest(args) => {
            invalidate_verified_resolution_manifest(args).await
        }
        ExecutionCommand::InvalidateVerifiedResolutionTopologyBoundary(args) => {
            invalidate_verified_resolution_topology_boundary(args).await
        }
        ExecutionCommand::InvalidateVerifiedResolutionRecordBoundary(args) => {
            invalidate_verified_resolution_record_boundary(args).await
        }
        ExecutionCommand::InvalidateVerifiedPrimaryNameManifest(args) => {
            invalidate_verified_primary_name_manifest(args).await
        }
        ExecutionCommand::InvalidateVerifiedPrimaryNameTopologyBoundary(args) => {
            invalidate_verified_primary_name_topology_boundary(args).await
        }
        ExecutionCommand::InvalidateVerifiedPrimaryNameRecordBoundary(args) => {
            invalidate_verified_primary_name_record_boundary(args).await
        }
    }
}

async fn permissions_current(args: PermissionsCurrentArgs) -> Result<()> {
    match args.command {
        PermissionsCurrentCommand::Rebuild(args) => rebuild_permissions_current(args).await,
    }
}

async fn record_inventory_current(args: RecordInventoryCurrentArgs) -> Result<()> {
    match args.command {
        RecordInventoryCurrentCommand::Rebuild(args) => {
            rebuild_record_inventory_current(args).await
        }
    }
}

async fn primary_names_current(args: PrimaryNamesCurrentArgs) -> Result<()> {
    match args.command {
        PrimaryNamesCurrentCommand::Rebuild(args) => rebuild_primary_names_current(args).await,
    }
}

async fn replay_command(args: ReplayArgs) -> Result<()> {
    match args.command {
        ReplayCommand::AllCurrentProjections(args) => replay_all_current_projections(args).await,
    }
}

async fn resolver_current(args: ResolverCurrentArgs) -> Result<()> {
    match args.command {
        ResolverCurrentCommand::Rebuild(args) => rebuild_resolver_current(args).await,
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

async fn invalidate_verified_resolution_manifest(
    args: InvalidateVerifiedResolutionManifestArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary = execution::invalidate_verified_resolution_manifest_version(
        &pool,
        &execution::VerifiedResolutionManifestInvalidation {
            namespace: args.namespace.clone(),
            source_manifest_id: args.source_manifest_id,
            source_family: args.source_family.clone(),
            manifest_version: args.manifest_version,
        },
    )
    .await?;

    info!(
        service = "worker",
        execution_request_type = "verified_resolution",
        invalidation_cause = "manifest_version",
        namespace = args.namespace.as_str(),
        manifest_version = args.manifest_version,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_resolution execution outcome invalidation completed"
    );

    Ok(())
}

async fn invalidate_verified_resolution_topology_boundary(
    args: InvalidateVerifiedResolutionBoundaryArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let invalidation = execution::VerifiedResolutionBoundaryInvalidation {
        namespace: args.namespace.clone(),
        logical_name_id: args.logical_name_id.clone(),
        resource_id: args.resource_id,
        normalized_event_id: args.normalized_event_id,
        event_kind: args.event_kind.clone(),
        chain_id: args.chain_id.clone(),
        block_number: args.block_number,
        block_hash: args.block_hash.clone(),
        timestamp: args.timestamp.clone(),
    };
    let summary =
        execution::invalidate_verified_resolution_topology_boundary(&pool, &invalidation).await?;

    info!(
        service = "worker",
        execution_request_type = "verified_resolution",
        invalidation_cause = "topology_boundary",
        namespace = args.namespace.as_str(),
        logical_name_id = args.logical_name_id.as_str(),
        resource_id = %args.resource_id,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_resolution topology invalidation completed"
    );

    Ok(())
}

async fn invalidate_verified_resolution_record_boundary(
    args: InvalidateVerifiedResolutionBoundaryArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let invalidation = execution::VerifiedResolutionBoundaryInvalidation {
        namespace: args.namespace.clone(),
        logical_name_id: args.logical_name_id.clone(),
        resource_id: args.resource_id,
        normalized_event_id: args.normalized_event_id,
        event_kind: args.event_kind.clone(),
        chain_id: args.chain_id.clone(),
        block_number: args.block_number,
        block_hash: args.block_hash.clone(),
        timestamp: args.timestamp.clone(),
    };
    let summary =
        execution::invalidate_verified_resolution_record_boundary(&pool, &invalidation).await?;

    info!(
        service = "worker",
        execution_request_type = "verified_resolution",
        invalidation_cause = "record_boundary",
        namespace = args.namespace.as_str(),
        logical_name_id = args.logical_name_id.as_str(),
        resource_id = %args.resource_id,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_resolution record invalidation completed"
    );

    Ok(())
}

async fn invalidate_verified_primary_name_manifest(
    args: InvalidateVerifiedPrimaryNameManifestArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary = execution::invalidate_verified_primary_name_manifest_version(
        &pool,
        &execution::VerifiedPrimaryNameManifestInvalidation {
            namespace: args.namespace.clone(),
            address: args.address.clone(),
            coin_type: args.coin_type.clone(),
            source_manifest_id: args.source_manifest_id,
            source_family: args.source_family.clone(),
            manifest_version: args.manifest_version,
        },
    )
    .await?;

    info!(
        service = "worker",
        execution_request_type = "verified_primary_name",
        invalidation_cause = "manifest_version",
        namespace = args.namespace.as_str(),
        address = args.address.as_str(),
        coin_type = args.coin_type.as_str(),
        manifest_version = args.manifest_version,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_primary_name execution outcome invalidation completed"
    );

    Ok(())
}

async fn invalidate_verified_primary_name_topology_boundary(
    args: InvalidateVerifiedPrimaryNameBoundaryArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let invalidation = execution::VerifiedPrimaryNameBoundaryInvalidation {
        namespace: args.namespace.clone(),
        address: args.address.clone(),
        coin_type: args.coin_type.clone(),
        logical_name_id: args.logical_name_id.clone(),
        resource_id: args.resource_id,
        normalized_event_id: args.normalized_event_id,
        event_kind: args.event_kind.clone(),
        chain_id: args.chain_id.clone(),
        block_number: args.block_number,
        block_hash: args.block_hash.clone(),
        timestamp: args.timestamp.clone(),
    };
    let summary =
        execution::invalidate_verified_primary_name_topology_boundary(&pool, &invalidation).await?;

    info!(
        service = "worker",
        execution_request_type = "verified_primary_name",
        invalidation_cause = "topology_boundary",
        namespace = args.namespace.as_str(),
        address = args.address.as_str(),
        coin_type = args.coin_type.as_str(),
        logical_name_id = args.logical_name_id.as_str(),
        resource_id = %args.resource_id,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_primary_name topology invalidation completed"
    );

    Ok(())
}

async fn invalidate_verified_primary_name_record_boundary(
    args: InvalidateVerifiedPrimaryNameBoundaryArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let invalidation = execution::VerifiedPrimaryNameBoundaryInvalidation {
        namespace: args.namespace.clone(),
        address: args.address.clone(),
        coin_type: args.coin_type.clone(),
        logical_name_id: args.logical_name_id.clone(),
        resource_id: args.resource_id,
        normalized_event_id: args.normalized_event_id,
        event_kind: args.event_kind.clone(),
        chain_id: args.chain_id.clone(),
        block_number: args.block_number,
        block_hash: args.block_hash.clone(),
        timestamp: args.timestamp.clone(),
    };
    let summary =
        execution::invalidate_verified_primary_name_record_boundary(&pool, &invalidation).await?;

    info!(
        service = "worker",
        execution_request_type = "verified_primary_name",
        invalidation_cause = "record_boundary",
        namespace = args.namespace.as_str(),
        address = args.address.as_str(),
        coin_type = args.coin_type.as_str(),
        logical_name_id = args.logical_name_id.as_str(),
        resource_id = %args.resource_id,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_primary_name record invalidation completed"
    );

    Ok(())
}

async fn rebuild_permissions_current(args: PermissionsCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary =
        permissions::rebuild_permissions_current(&pool, args.resource_id.as_deref()).await?;

    info!(
        service = "worker",
        projection = "permissions_current",
        requested_resource_count = summary.requested_resource_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        resource_id = args.resource_id.as_deref().unwrap_or("all"),
        "permissions_current rebuild completed"
    );

    Ok(())
}

async fn rebuild_primary_names_current(args: PrimaryNamesCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary = primary_name::rebuild_primary_names_current(
        &pool,
        args.address.as_deref(),
        args.namespace.as_deref(),
        args.coin_type.as_deref(),
    )
    .await?;

    info!(
        service = "worker",
        projection = "primary_names_current",
        requested_tuple_count = summary.requested_tuple_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        success_row_count = summary.success_row_count,
        not_found_row_count = summary.not_found_row_count,
        invalid_name_row_count = summary.invalid_name_row_count,
        address = args.address.as_deref().unwrap_or("all"),
        namespace = args.namespace.as_deref().unwrap_or("all"),
        coin_type = args.coin_type.as_deref().unwrap_or("all"),
        "primary_names_current rebuild completed"
    );

    Ok(())
}

async fn replay_all_current_projections(args: AllCurrentProjectionsArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary = replay::rebuild_all_current_projections(&pool).await?;

    if args.json {
        let payload = summary
            .json_summary_string()
            .context("failed to serialize all-current-projections replay JSON summary")?;
        println!("{payload}");
        return Ok(());
    }

    info!(
        service = "worker",
        replay = "all_current_projections",
        projection_order = ?summary.projection_order(),
        projection_count = summary.steps.len(),
        total_upserted_row_count = summary.total_upserted_row_count(),
        total_deleted_row_count = summary.total_deleted_row_count(),
        "all current projections replay completed"
    );

    Ok(())
}

async fn rebuild_record_inventory_current(args: RecordInventoryCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary =
        record_inventory::rebuild_record_inventory_current(&pool, args.resource_id.as_deref())
            .await?;

    info!(
        service = "worker",
        projection = "record_inventory_current",
        requested_resource_count = summary.requested_resource_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        resource_id = args.resource_id.as_deref().unwrap_or("all"),
        "record_inventory_current rebuild completed"
    );

    Ok(())
}

async fn rebuild_resolver_current(args: ResolverCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary = resolver::rebuild_resolver_current(
        &pool,
        args.chain_id.as_deref(),
        args.resolver_address.as_deref(),
    )
    .await?;

    info!(
        service = "worker",
        projection = "resolver_current",
        requested_resolver_count = summary.requested_resolver_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        chain_id = args.chain_id.as_deref().unwrap_or("all"),
        resolver_address = args.resolver_address.as_deref().unwrap_or("all"),
        "resolver_current rebuild completed"
    );

    Ok(())
}

impl Cli {
    fn writes_machine_json(&self) -> bool {
        matches!(
            &self.command,
            Command::Inspect(_)
                | Command::Replay(ReplayArgs {
                    command: ReplayCommand::AllCurrentProjections(AllCurrentProjectionsArgs {
                        json: true,
                        ..
                    })
                })
        )
    }
}

fn init_tracing(service: &'static str, emit_logs_to_stderr: bool) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if std::env::var_os("BIGNAME_LOG_JSON").is_some() {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_target(false);
        if emit_logs_to_stderr {
            subscriber.with_writer(std::io::stderr).init();
        } else {
            subscriber.init();
        }
    } else {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .with_target(false);
        if emit_logs_to_stderr {
            subscriber.with_writer(std::io::stderr).init();
        } else {
            subscriber.init();
        }
    }

    info!(
        service = service,
        phase = bigname_domain::bootstrap_phase(),
        "logging configured"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_all_current_projections_cli_is_available() {
        let cli = Cli::parse_from(["bigname-worker", "replay", "all-current-projections"]);
        assert!(!cli.writes_machine_json());

        match cli.command {
            Command::Replay(args) => match args.command {
                ReplayCommand::AllCurrentProjections(args) => {
                    assert!(!args.json);
                }
            },
            other => panic!("expected replay command, got {other:?}"),
        }
    }

    #[test]
    fn replay_all_current_projections_json_flag_is_available() {
        let cli = Cli::parse_from([
            "bigname-worker",
            "replay",
            "all-current-projections",
            "--json",
        ]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Replay(args) => match args.command {
                ReplayCommand::AllCurrentProjections(args) => {
                    assert!(args.json);
                }
            },
            other => panic!("expected replay command, got {other:?}"),
        }
    }

    #[test]
    fn inspect_canonicality_cli_is_available() {
        let cli = Cli::parse_from([
            "bigname-worker",
            "inspect",
            "canonicality",
            "--chain-id",
            "eth-mainnet",
            "--block-hash",
            "0xabc",
        ]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Inspect(args) => match args.command {
                inspect::InspectCommand::Canonicality(args) => {
                    assert_eq!(args.chain_id, "eth-mainnet");
                    assert_eq!(args.block_hash, "0xabc");
                }
                other => panic!("expected canonicality inspect command, got {other:?}"),
            },
            other => panic!("expected inspect command, got {other:?}"),
        }
    }

    #[test]
    fn inspect_backfill_job_cli_is_available() {
        let cli = Cli::parse_from([
            "bigname-worker",
            "inspect",
            "backfill-job",
            "--backfill-job-id",
            "42",
        ]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Inspect(args) => match args.command {
                inspect::InspectCommand::BackfillJob(args) => {
                    assert_eq!(args.backfill_job_id, 42);
                }
                other => panic!("expected backfill job inspect command, got {other:?}"),
            },
            other => panic!("expected inspect command, got {other:?}"),
        }
    }

    #[test]
    fn inspect_execution_trace_cli_is_available() {
        let trace_id = "0e7ec7ac-e000-0000-0000-000000000abc";
        let cli = Cli::parse_from([
            "bigname-worker",
            "inspect",
            "execution-trace",
            "--execution-trace-id",
            trace_id,
            "--json",
        ]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Inspect(args) => match args.command {
                inspect::InspectCommand::ExecutionTrace(args) => {
                    assert_eq!(args.execution_trace_id.to_string(), trace_id);
                    assert!(args.json);
                }
                other => panic!("expected execution trace inspect command, got {other:?}"),
            },
            other => panic!("expected inspect command, got {other:?}"),
        }
    }

    #[test]
    fn inspect_stored_lineage_range_cli_is_available() {
        let cli = Cli::parse_from([
            "bigname-worker",
            "inspect",
            "stored-lineage-range",
            "--chain-id",
            "eth-mainnet",
            "--range-start-block-number",
            "10",
            "--range-end-block-number",
            "12",
        ]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Inspect(args) => match args.command {
                inspect::InspectCommand::StoredLineageRange(args) => {
                    assert_eq!(args.chain_id, "eth-mainnet");
                    assert_eq!(args.range_start_block_number, 10);
                    assert_eq!(args.range_end_block_number, 12);
                }
                other => panic!("expected stored lineage range inspect command, got {other:?}"),
            },
            other => panic!("expected inspect command, got {other:?}"),
        }
    }
}
