use std::future::Future;

use anyhow::{Context, Result};
use sqlx::PgPool;
use tracing::info;

use crate::{
    address_names, children, name_current, permissions, primary_name, record_inventory, resolver,
};

use super::{
    ALL_CURRENT_PROJECTION_ORDER, AllCurrentProjectionsReplaySummary,
    CurrentProjectionReplayStepSummary,
    progress::{
        clear_projection_replay_completed, mark_projection_replay_completed,
        projection_replay_completed,
    },
};

pub async fn rebuild_all_current_projections(
    pool: &PgPool,
) -> Result<AllCurrentProjectionsReplaySummary> {
    rebuild_all_current_projections_inner(pool, None, false).await
}

pub async fn rebuild_pending_all_current_projections(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
) -> Result<AllCurrentProjectionsReplaySummary> {
    rebuild_all_current_projections_inner(pool, normalized_target_block, true).await
}

async fn rebuild_all_current_projections_inner(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    skip_completed: bool,
) -> Result<AllCurrentProjectionsReplaySummary> {
    let mut steps = Vec::with_capacity(ALL_CURRENT_PROJECTION_ORDER.len());

    macro_rules! replay_step {
        ($projection:literal, $future:expr, $requested_field:ident) => {
            replay_projection_step(
                pool,
                $projection,
                normalized_target_block,
                skip_completed,
                async {
                    let summary = $future
                        .await
                        .context(concat!("failed to replay ", $projection))?;
                    Ok(CurrentProjectionReplayStepSummary {
                        projection: $projection,
                        requested_key_count: summary.$requested_field,
                        upserted_row_count: summary.upserted_row_count,
                        deleted_row_count: summary.deleted_row_count,
                    })
                },
            )
            .await?
        };
    }

    steps.push(replay_step!(
        "name_current",
        name_current::rebuild_name_current(pool, None),
        requested_name_count
    ));
    steps.push(replay_step!(
        "children_current",
        children::rebuild_children_current(pool, None),
        requested_parent_count
    ));
    steps.push(replay_step!(
        "permissions_current",
        permissions::rebuild_permissions_current(pool, None),
        requested_resource_count
    ));
    steps.push(replay_step!(
        "record_inventory_current",
        record_inventory::rebuild_record_inventory_current(pool, None),
        requested_resource_count
    ));
    steps.push(replay_step!(
        "resolver_current",
        resolver::rebuild_resolver_current(pool, None, None),
        requested_resolver_count
    ));
    steps.push(replay_step!(
        "address_names_current",
        address_names::rebuild_address_names_current(pool, None),
        requested_address_count
    ));
    steps.push(replay_step!(
        "primary_names_current",
        primary_name::rebuild_primary_names_current(pool, None, None, None),
        requested_tuple_count
    ));

    debug_assert_eq!(
        steps.iter().map(|step| step.projection).collect::<Vec<_>>(),
        ALL_CURRENT_PROJECTION_ORDER
    );

    Ok(AllCurrentProjectionsReplaySummary { steps })
}

async fn replay_projection_step<Fut>(
    pool: &PgPool,
    projection: &'static str,
    normalized_target_block: Option<i64>,
    skip_completed: bool,
    rebuild: Fut,
) -> Result<CurrentProjectionReplayStepSummary>
where
    Fut: Future<Output = Result<CurrentProjectionReplayStepSummary>>,
{
    if projection_should_replay(pool, projection, skip_completed, normalized_target_block).await? {
        let step = rebuild.await?;
        mark_projection_replay_completed(pool, &step, normalized_target_block).await?;
        Ok(step)
    } else {
        Ok(skipped_step(projection))
    }
}

async fn projection_should_replay(
    pool: &PgPool,
    projection: &'static str,
    skip_completed: bool,
    normalized_target_block: Option<i64>,
) -> Result<bool> {
    if skip_completed && projection_replay_completed(pool, projection).await? {
        info!(
            service = "worker",
            replay = "all_current_projections",
            projection,
            normalized_target_block,
            "all-current projection replay skipped because durable completion marker exists"
        );
        return Ok(false);
    }
    clear_projection_replay_completed(pool, projection).await?;
    Ok(true)
}

fn skipped_step(projection: &'static str) -> CurrentProjectionReplayStepSummary {
    CurrentProjectionReplayStepSummary {
        projection,
        requested_key_count: 0,
        upserted_row_count: 0,
        deleted_row_count: 0,
    }
}
