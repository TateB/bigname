use anyhow::{Context, Result};
use tracing::info;

mod execution_invalidation;

use crate::cli::*;
use crate::{
    address_names, automatic_projection_replay, children, inspect, manifest_drift, name_current,
    permissions, primary_name, raw_facts, record_inventory, replay, resolver,
};
use execution_invalidation::execution_command;

pub(crate) async fn dispatch(command: Command) -> Result<()> {
    match command {
        Command::Run(args) => automatic_projection_replay::run_worker(args).await,
        Command::Migrate(args) => migrate(args).await,
        Command::AddressNamesCurrent(args) => address_names_current(args).await,
        Command::ChildrenCurrent(args) => children_current(args).await,
        Command::Execution(args) => execution_command(args).await,
        Command::Inspect(args) => inspect::inspect_command(args).await,
        Command::ManifestDrift(args) => manifest_drift_command(args).await,
        Command::NameCurrent(args) => name_current(args).await,
        Command::PermissionsCurrent(args) => permissions_current(args).await,
        Command::PrimaryNamesCurrent(args) => primary_names_current(args).await,
        Command::RawFacts(args) => raw_facts_command(args).await,
        Command::Replay(args) => replay_command(args).await,
        Command::RecordInventoryCurrent(args) => record_inventory_current(args).await,
        Command::ResolverCurrent(args) => resolver_current(args).await,
    }
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

async fn manifest_drift_command(args: ManifestDriftArgs) -> Result<()> {
    match args.command {
        ManifestDriftCommand::Audit(args) => manifest_drift::audit(args).await,
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
        RecordInventoryCurrentCommand::HydrateTextValues(args) => {
            hydrate_record_inventory_text_values(args).await
        }
    }
}

async fn primary_names_current(args: PrimaryNamesCurrentArgs) -> Result<()> {
    match args.command {
        PrimaryNamesCurrentCommand::Rebuild(args) => rebuild_primary_names_current(args).await,
        PrimaryNamesCurrentCommand::HydrateLegacyReverseResolver(args) => {
            hydrate_primary_names_legacy_reverse_resolver(args).await
        }
    }
}

async fn raw_facts_command(args: RawFactsArgs) -> Result<()> {
    match args.command {
        RawFactsCommand::CompactLogStaging(args) => raw_facts::compact_log_staging(args).await,
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

async fn hydrate_primary_names_legacy_reverse_resolver(
    args: PrimaryNamesCurrentHydrateLegacyReverseResolverArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let hydration_config = primary_name_legacy_reverse_hydration_config(
        &args.chain_rpc_urls,
        args.multicall3_address,
        args.batch_size,
        &args.legacy_reverse_resolver_addresses,
    )?;
    let summary =
        primary_name::hydrate_legacy_reverse_resolver_primary_names(&pool, hydration_config)
            .await?;
    primary_name::log_legacy_reverse_hydration_summary(&summary);
    Ok(())
}

async fn replay_all_current_projections(args: AllCurrentProjectionsArgs) -> Result<()> {
    let database =
        automatic_projection_replay::all_current_projections_database_config(args.database);
    let pool = bigname_storage::connect(&database).await?;
    let text_hydration_config = optional_text_hydration_config(
        &args.chain_rpc_urls,
        args.text_hydration_multicall3_address.clone(),
        args.text_hydration_batch_size,
    )?;
    let primary_hydration_config = optional_primary_name_legacy_reverse_hydration_config(
        &args.chain_rpc_urls,
        args.legacy_reverse_hydration_multicall3_address,
        args.legacy_reverse_hydration_batch_size,
        &args.legacy_reverse_resolver_addresses,
    )?;
    let summary = replay::rebuild_all_current_projections(
        &pool,
        text_hydration_config.as_ref(),
        primary_hydration_config.as_ref(),
    )
    .await?;

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
        total_requested_key_count = summary.total_requested_key_count(),
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

    if args.hydrate_text_values {
        let hydration_config = text_hydration_config(
            &args.chain_rpc_urls,
            args.text_hydration_multicall3_address,
            args.text_hydration_batch_size,
        )?;
        let hydration_summary = record_inventory::hydrate_record_inventory_text_values(
            &pool,
            args.resource_id.as_deref(),
            hydration_config,
        )
        .await?;
        record_inventory::log_text_hydration_summary(
            args.resource_id.as_deref(),
            &hydration_summary,
        );
    }

    Ok(())
}

async fn hydrate_record_inventory_text_values(
    args: RecordInventoryCurrentHydrateTextValuesArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let hydration_config = text_hydration_config(
        &args.chain_rpc_urls,
        args.multicall3_address,
        args.batch_size,
    )?;
    let summary = record_inventory::hydrate_record_inventory_text_values(
        &pool,
        args.resource_id.as_deref(),
        hydration_config,
    )
    .await?;
    record_inventory::log_text_hydration_summary(args.resource_id.as_deref(), &summary);
    Ok(())
}

fn text_hydration_config(
    chain_rpc_url_entries: &[String],
    multicall3_address: String,
    batch_size: usize,
) -> Result<record_inventory::RecordInventoryTextHydrationConfig> {
    record_inventory::RecordInventoryTextHydrationConfig::from_chain_rpc_url_entries(
        chain_rpc_url_entries,
        multicall3_address,
        batch_size,
    )?
    .context("text hydration requires --chain-rpc-url <chain>=<url>")
}

fn optional_text_hydration_config(
    chain_rpc_url_entries: &[String],
    multicall3_address: String,
    batch_size: usize,
) -> Result<Option<record_inventory::RecordInventoryTextHydrationConfig>> {
    record_inventory::RecordInventoryTextHydrationConfig::from_chain_rpc_url_entries(
        chain_rpc_url_entries,
        multicall3_address,
        batch_size,
    )
}

fn primary_name_legacy_reverse_hydration_config(
    chain_rpc_url_entries: &[String],
    multicall3_address: String,
    batch_size: usize,
    extra_resolver_addresses: &[String],
) -> Result<primary_name::PrimaryNameLegacyReverseHydrationConfig> {
    primary_name::PrimaryNameLegacyReverseHydrationConfig::from_chain_rpc_url_entries(
        chain_rpc_url_entries,
        multicall3_address,
        batch_size,
        extra_resolver_addresses,
    )?
    .context(
        "legacy reverse-resolver primary-name hydration requires --chain-rpc-url <chain>=<url>",
    )
}

fn optional_primary_name_legacy_reverse_hydration_config(
    chain_rpc_url_entries: &[String],
    multicall3_address: String,
    batch_size: usize,
    extra_resolver_addresses: &[String],
) -> Result<Option<primary_name::PrimaryNameLegacyReverseHydrationConfig>> {
    primary_name::PrimaryNameLegacyReverseHydrationConfig::from_chain_rpc_url_entries(
        chain_rpc_url_entries,
        multicall3_address,
        batch_size,
        extra_resolver_addresses,
    )
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
