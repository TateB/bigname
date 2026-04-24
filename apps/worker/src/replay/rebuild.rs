use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::{
    address_names, children, name_current, permissions, primary_name, record_inventory, resolver,
};

use super::{
    ALL_CURRENT_PROJECTION_ORDER, AllCurrentProjectionsReplaySummary,
    CurrentProjectionReplayStepSummary,
};

pub async fn rebuild_all_current_projections(
    pool: &PgPool,
) -> Result<AllCurrentProjectionsReplaySummary> {
    let mut steps = Vec::with_capacity(ALL_CURRENT_PROJECTION_ORDER.len());

    let summary = name_current::rebuild_name_current(pool, None)
        .await
        .context("failed to replay name_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "name_current",
        requested_key_count: summary.requested_name_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = children::rebuild_children_current(pool, None)
        .await
        .context("failed to replay children_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "children_current",
        requested_key_count: summary.requested_parent_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = permissions::rebuild_permissions_current(pool, None)
        .await
        .context("failed to replay permissions_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "permissions_current",
        requested_key_count: summary.requested_resource_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = record_inventory::rebuild_record_inventory_current(pool, None)
        .await
        .context("failed to replay record_inventory_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "record_inventory_current",
        requested_key_count: summary.requested_resource_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = resolver::rebuild_resolver_current(pool, None, None)
        .await
        .context("failed to replay resolver_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "resolver_current",
        requested_key_count: summary.requested_resolver_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = address_names::rebuild_address_names_current(pool, None)
        .await
        .context("failed to replay address_names_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "address_names_current",
        requested_key_count: summary.requested_address_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = primary_name::rebuild_primary_names_current(pool, None, None, None)
        .await
        .context("failed to replay primary_names_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "primary_names_current",
        requested_key_count: summary.requested_tuple_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    debug_assert_eq!(
        steps.iter().map(|step| step.projection).collect::<Vec<_>>(),
        ALL_CURRENT_PROJECTION_ORDER
    );

    Ok(AllCurrentProjectionsReplaySummary { steps })
}
