use anyhow::Result;
use tracing::info;

use crate::{cli::*, execution};

pub(super) async fn execution_command(args: ExecutionArgs) -> Result<()> {
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
