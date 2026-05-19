use anyhow::{Context, Result};
use bigname_storage::{
    RecordInventoryCurrentRow, clear_record_inventory_current, upsert_record_inventory_current_rows,
};
use futures_util::{TryStreamExt, pin_mut};
use serde_json::{Value, json};
use sqlx::{PgPool, types::time::OffsetDateTime};
use tokio::task::JoinSet;
use uuid::Uuid;

use super::{
    chain_position::{
        build_chain_positions, build_record_version_boundary, collect_chain_position_events,
        load_basenames_transport_chain_positions,
    },
    constants::*,
    json::{
        build_canonicality_summary, build_coverage, build_entries, build_explicit_gaps,
        build_last_change, build_provenance, build_selectors, build_unsupported_families,
        gap_value, resolver_family_status_value,
    },
    loading::{load_relevant_events, stream_target_resource_ids},
    profile::{ResolverProfileGate, ResolverRecordFamilyStatuses, resolver_local_source_family},
    types::{RecordInventoryCurrentRebuildSummary, RelevantEvent},
};

mod profile_rows;

use profile_rows::{
    build_profile_gated_row, build_row_coverage, build_row_unsupported_families,
    filter_explicit_gaps,
};

const RECORD_INVENTORY_CURRENT_REBUILD_BATCH_SIZE: usize = 500;
const RECORD_INVENTORY_CURRENT_REBUILD_CONCURRENCY: usize = 8;

pub(super) async fn rebuild_record_inventory_current(
    pool: &PgPool,
    resource_id: Option<&str>,
) -> Result<RecordInventoryCurrentRebuildSummary> {
    match resource_id {
        Some(resource_id) => rebuild_one_resource(pool, resource_id).await,
        None => rebuild_all_resources(pool).await,
    }
}

async fn rebuild_all_resources(pool: &PgPool) -> Result<RecordInventoryCurrentRebuildSummary> {
    let deleted_row_count = clear_record_inventory_current(pool).await?;
    let mut rows = Vec::with_capacity(RECORD_INVENTORY_CURRENT_REBUILD_BATCH_SIZE);
    let mut requested_resource_count = 0usize;
    let mut completed_resource_count = 0usize;
    let mut upserted_row_count = 0usize;

    let resource_ids = stream_target_resource_ids(pool);
    pin_mut!(resource_ids);
    let mut tasks = JoinSet::new();

    while tasks.len() < RECORD_INVENTORY_CURRENT_REBUILD_CONCURRENCY {
        let Some(resource_id) = resource_ids.try_next().await? else {
            break;
        };
        requested_resource_count += 1;
        spawn_record_inventory_rebuild_task(&mut tasks, pool, resource_id);
    }

    while let Some(result) = tasks.join_next().await {
        completed_resource_count += 1;
        if let Some(row) = result?? {
            rows.push(row);
        }

        if rows.len() >= RECORD_INVENTORY_CURRENT_REBUILD_BATCH_SIZE {
            upserted_row_count += upsert_record_inventory_current_rows(pool, &rows)
                .await?
                .len();
            rows.clear();
        }

        if completed_resource_count % 5_000 == 0 {
            tracing::info!(
                projection = "record_inventory_current",
                queued_resource_count = requested_resource_count,
                completed_resource_count,
                upserted_row_count,
                "record_inventory_current rebuild resources processed"
            );
        }

        while tasks.len() < RECORD_INVENTORY_CURRENT_REBUILD_CONCURRENCY {
            let Some(resource_id) = resource_ids.try_next().await? else {
                break;
            };
            requested_resource_count += 1;
            spawn_record_inventory_rebuild_task(&mut tasks, pool, resource_id);
        }
    }

    if !rows.is_empty() {
        upserted_row_count += upsert_record_inventory_current_rows(pool, &rows)
            .await?
            .len();
    }

    Ok(RecordInventoryCurrentRebuildSummary {
        requested_resource_count,
        upserted_row_count,
        deleted_row_count,
    })
}

fn spawn_record_inventory_rebuild_task(
    tasks: &mut JoinSet<Result<Option<RecordInventoryCurrentRow>>>,
    pool: &PgPool,
    resource_id: Uuid,
) {
    let pool = pool.clone();
    tasks.spawn(async move { build_row_for_resource(&pool, resource_id).await });
}

async fn build_row_for_resource(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let events = load_relevant_events(pool, resource_id).await?;
    let profile_gate = ResolverProfileGate::load_for_events(pool, &events).await?;
    build_row_from_events(pool, &profile_gate, resource_id, &events).await
}

async fn rebuild_one_resource(
    pool: &PgPool,
    resource_id: &str,
) -> Result<RecordInventoryCurrentRebuildSummary> {
    let resource_id = Uuid::parse_str(resource_id)
        .with_context(|| format!("resource_id must be a UUID: {resource_id}"))?;
    let events = load_relevant_events(pool, resource_id).await?;
    if events.is_empty() {
        let deleted_row_count =
            delete_record_inventory_rows_for_resource(pool, resource_id).await?;
        return Ok(RecordInventoryCurrentRebuildSummary {
            requested_resource_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let profile_gate = ResolverProfileGate::load_for_events(pool, &events).await?;
    let Some(row) = build_row_from_events(pool, &profile_gate, resource_id, &events).await? else {
        let deleted_row_count =
            delete_record_inventory_rows_for_resource(pool, resource_id).await?;
        return Ok(RecordInventoryCurrentRebuildSummary {
            requested_resource_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let upserted_row_count = upsert_record_inventory_current_rows(pool, std::slice::from_ref(&row))
        .await?
        .len();
    let deleted_row_count =
        delete_stale_record_inventory_current_rows_for_resource(pool, resource_id, &row).await?;
    Ok(RecordInventoryCurrentRebuildSummary {
        requested_resource_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn delete_record_inventory_rows_for_resource(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM record_inventory_current
        WHERE resource_id = $1
        "#,
    )
    .bind(resource_id)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete record_inventory_current rows for resource_id {resource_id}")
    })
    .map(|result| result.rows_affected())
}

async fn delete_stale_record_inventory_current_rows_for_resource(
    pool: &PgPool,
    resource_id: Uuid,
    row: &RecordInventoryCurrentRow,
) -> Result<u64> {
    let record_version_boundary = serde_json::to_string(&row.record_version_boundary)
        .context("failed to serialize record_inventory_current boundary for cleanup")?;

    sqlx::query(
        r#"
        DELETE FROM record_inventory_current current
        WHERE current.resource_id = $1
          AND current.record_version_boundary <> $2::JSONB
        "#,
    )
    .bind(resource_id)
    .bind(record_version_boundary)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to delete stale record_inventory_current rows for resource_id {resource_id}"
        )
    })
    .map(|result| result.rows_affected())
}

async fn build_row_from_events(
    pool: &PgPool,
    profile_gate: &ResolverProfileGate,
    resource_id: Uuid,
    events: &[RelevantEvent],
) -> Result<Option<RecordInventoryCurrentRow>> {
    if events.is_empty() {
        return Ok(None);
    }

    let latest_resolver_event = events
        .iter()
        .rev()
        .find(|event| event.event_kind == EVENT_KIND_RESOLVER_CHANGED);
    let latest_resolver_record_statuses = latest_resolver_event
        .and_then(|resolver_event| profile_gate.current_record_family_statuses(resolver_event));

    let boundary_index = events.iter().rposition(|event| {
        event.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED
            || event.event_kind == EVENT_KIND_RESOLVER_CHANGED
    });
    let scoped_events = &events[boundary_index.unwrap_or(0)..];
    let boundary_anchor = match boundary_index {
        Some(index) => events
            .get(index)
            .context("record_inventory_current rebuild boundary index out of range")?,
        None => events
            .last()
            .context("record_inventory_current rebuild requires at least one event")?,
    };
    let has_record_version_boundary_pointer =
        boundary_anchor.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED;
    let record_version_boundary =
        build_record_version_boundary(boundary_anchor, has_record_version_boundary_pointer)?;
    let record_change_events = scoped_events
        .iter()
        .filter(|event| {
            event.event_kind == EVENT_KIND_RECORD_CHANGED
                && profile_gate.allows_event_for_current_resolver(event, latest_resolver_event)
        })
        .collect::<Vec<_>>();
    if let Some(resolver_event) = latest_resolver_event
        && latest_resolver_record_statuses
            .as_ref()
            .is_some_and(|statuses| !statuses.any_supported())
        && record_change_events.is_empty()
    {
        return build_profile_gated_row(
            pool,
            resource_id,
            resolver_event,
            boundary_anchor,
            latest_resolver_record_statuses.as_ref(),
        )
        .await;
    }
    let provenance_events = scoped_events
        .iter()
        .filter(|event| {
            event.event_kind == EVENT_KIND_RESOLVER_CHANGED
                || resolver_local_source_family(&event.source_family).is_none()
                || profile_gate.allows_event_for_current_resolver(event, latest_resolver_event)
        })
        .cloned()
        .collect::<Vec<_>>();

    let selectors = build_selectors(&record_change_events)?;
    let explicit_gaps = filter_explicit_gaps(
        build_explicit_gaps(&selectors),
        latest_resolver_record_statuses.as_ref(),
    );
    let unsupported_families = build_row_unsupported_families(
        latest_resolver_record_statuses.as_ref(),
        &record_change_events,
    )?;
    let entries = build_entries(&record_change_events, &selectors)?;
    let last_change = provenance_events
        .last()
        .map(build_last_change)
        .transpose()?;
    let chain_position_events = collect_chain_position_events(boundary_anchor, &provenance_events);
    let supplemental_chain_positions =
        load_basenames_transport_chain_positions(pool, &chain_position_events).await?;

    Ok(Some(RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary,
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
        selectors: Value::Array(
            selectors
                .into_values()
                .map(|selector| {
                    json!({
                        "record_key": selector.record_key,
                        "record_family": selector.record_family,
                        "selector_key": selector.selector_key,
                        "cacheable": true,
                    })
                })
                .collect(),
        ),
        explicit_gaps: Value::Array(explicit_gaps),
        unsupported_families: Value::Array(unsupported_families),
        last_change,
        entries: Value::Array(entries),
        provenance: build_provenance(&provenance_events)?,
        coverage: build_row_coverage(
            latest_resolver_record_statuses.as_ref(),
            boundary_anchor,
            &provenance_events,
        ),
        chain_positions: build_chain_positions(
            &chain_position_events,
            supplemental_chain_positions,
        ),
        canonicality_summary: build_canonicality_summary(&provenance_events),
        manifest_version: provenance_events
            .iter()
            .map(|event| event.manifest_version)
            .max()
            .unwrap_or(1),
        last_recomputed_at: provenance_events
            .iter()
            .filter_map(|event| event.block_timestamp)
            .max()
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
    }))
}
