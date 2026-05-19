use std::{collections::BTreeSet, time::Instant};

use anyhow::{Context, Result, bail};
use bigname_storage::list_canonical_raw_log_replay_inputs_for_block_hashes;
use sqlx::Row;
use tracing::info;

#[path = "replay/classification.rs"]
mod classification;
#[path = "replay/profile_scope.rs"]
mod profile_scope;
#[path = "replay/scoped.rs"]
mod scoped;

use super::{
    adapter_sync::sync_replay_normalized_events_from_persisted_raw_payloads,
    types::{
        PersistedRawPayloadAdapterSyncSummary, RawFactNormalizedEventReplayOutcome,
        RawFactNormalizedEventReplayRequest, RawFactNormalizedEventReplaySelection,
    },
};
use classification::classify_raw_fact_replay_contract;
use profile_scope::{
    ensure_replay_matches_deployment_profile_scope, load_replay_adapter_source_scope,
};
use scoped::load_replay_raw_log_selection_for_scoped_range;

pub(crate) use classification::{
    NormalizedEventReplayAdapter, RawFactReplayContractPlan,
    active_closure_or_dependency_replay_adapters, chain_has_closure_or_dependency_replay_adapter,
    source_scope_includes_adapter, unsupported_closure_replay_adapters,
};

pub(crate) async fn replay_raw_fact_normalized_events(
    pool: &sqlx::PgPool,
    request: RawFactNormalizedEventReplayRequest,
) -> Result<RawFactNormalizedEventReplayOutcome> {
    if request.deployment_profile.trim().is_empty() {
        bail!("deployment_profile must not be empty");
    }

    let total_started = Instant::now();
    let selection_kind = request.selection.as_str();
    let source_scope_target_count = request.selection.source_scope_target_count();
    let selection_started = Instant::now();
    let raw_log_selection = load_replay_raw_log_selection(pool, &request).await?;
    let load_selection_ms = selection_started.elapsed().as_millis();

    let profile_scope_started = Instant::now();
    ensure_replay_matches_deployment_profile_scope(pool, &request, raw_log_selection.range).await?;
    let profile_scope_ms = profile_scope_started.elapsed().as_millis();

    let source_scope_started = Instant::now();
    let source_scope = load_replay_adapter_source_scope(
        pool,
        &request,
        raw_log_selection.range,
        &raw_log_selection.address_targets,
    )
    .await?;
    let source_scope_ms = source_scope_started.elapsed().as_millis();

    let replay_contract_plan =
        classify_raw_fact_replay_contract(pool, &request, &raw_log_selection, &source_scope)
            .await?;

    let adapter_sync_started = Instant::now();
    let normalized_event_summary = if raw_log_selection.block_hashes.is_empty() {
        PersistedRawPayloadAdapterSyncSummary::default()
    } else if source_scope.is_empty() {
        PersistedRawPayloadAdapterSyncSummary {
            scanned_log_count: raw_log_selection.canonical_raw_log_count,
            matched_log_count: 0,
            total_synced_count: 0,
            total_inserted_count: 0,
        }
    } else {
        sync_replay_normalized_events_from_persisted_raw_payloads(
            pool,
            &request.chain,
            &raw_log_selection.block_hashes,
            Some(&source_scope),
            raw_log_selection.canonical_raw_log_count,
            replay_contract_plan,
        )
        .await?
    };
    let adapter_sync_ms = adapter_sync_started.elapsed().as_millis();

    info!(
        service = "indexer",
        replay_cursor_kind = "raw_fact_normalized_events",
        deployment_profile = %request.deployment_profile,
        chain = %request.chain,
        selection_kind,
        requested_source_scope_target_count = source_scope_target_count,
        selected_block_count = raw_log_selection.block_hashes.len(),
        address_target_count = raw_log_selection.address_targets.len(),
        replay_source_scope_target_count = source_scope.len(),
        closure_or_dependency_replay = replay_contract_plan.permits_nonstateless_adapters(),
        canonical_raw_log_count = raw_log_selection.canonical_raw_log_count,
        scanned_raw_log_count = normalized_event_summary.scanned_log_count,
        matched_raw_log_count = normalized_event_summary.matched_log_count,
        normalized_event_synced_count = normalized_event_summary.total_synced_count,
        normalized_event_inserted_count = normalized_event_summary.total_inserted_count,
        load_selection_ms,
        profile_scope_ms,
        source_scope_ms,
        adapter_sync_ms,
        elapsed_ms = total_started.elapsed().as_millis(),
        "raw-fact normalized-event replay timing completed"
    );

    Ok(RawFactNormalizedEventReplayOutcome {
        deployment_profile: request.deployment_profile,
        chain: request.chain,
        selection_kind,
        source_scope_target_count,
        selected_block_count: raw_log_selection.block_hashes.len(),
        canonical_raw_log_count: raw_log_selection.canonical_raw_log_count,
        scanned_raw_log_count: normalized_event_summary.scanned_log_count,
        matched_raw_log_count: normalized_event_summary.matched_log_count,
        normalized_event_synced_count: normalized_event_summary.total_synced_count,
        normalized_event_inserted_count: normalized_event_summary.total_inserted_count,
    })
}

#[derive(Debug, Eq, PartialEq)]
struct ReplayRawLogSelection {
    range: Option<(i64, i64)>,
    block_hashes: Vec<String>,
    address_targets: Vec<(String, String)>,
    canonical_raw_log_count: usize,
}

async fn load_replay_raw_log_selection(
    pool: &sqlx::PgPool,
    request: &RawFactNormalizedEventReplayRequest,
) -> Result<ReplayRawLogSelection> {
    match &request.selection {
        RawFactNormalizedEventReplaySelection::BlockRange {
            from_block,
            to_block,
        } => {
            validate_replay_block_range(*from_block, *to_block)?;
            load_replay_raw_log_selection_for_range(pool, &request.chain, *from_block, *to_block)
                .await
        }
        RawFactNormalizedEventReplaySelection::ScopedBlockRange {
            from_block,
            to_block,
            source_scope,
        } => {
            validate_replay_block_range(*from_block, *to_block)?;
            load_replay_raw_log_selection_for_scoped_range(
                pool,
                &request.chain,
                *from_block,
                *to_block,
                source_scope,
            )
            .await
        }
        RawFactNormalizedEventReplaySelection::BlockHashes(block_hashes) => {
            let raw_logs = list_canonical_raw_log_replay_inputs_for_block_hashes(
                pool,
                &request.chain,
                block_hashes,
            )
            .await?;
            let range = replay_manifest_scope_range_for_raw_logs(&raw_logs)?;
            let block_hashes = raw_logs
                .iter()
                .map(|raw_log| raw_log.block_hash.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let address_targets = raw_logs
                .iter()
                .map(|raw_log| {
                    (
                        request.chain.clone(),
                        raw_log.emitting_address.to_ascii_lowercase(),
                    )
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();

            Ok(ReplayRawLogSelection {
                range,
                block_hashes,
                address_targets,
                canonical_raw_log_count: raw_logs.len(),
            })
        }
    }
}

fn validate_replay_block_range(from_block: i64, to_block: i64) -> Result<()> {
    if from_block < 0 || to_block < 0 {
        bail!(
            "raw-fact normalized-event replay range must be non-negative, got {from_block}..={to_block}"
        );
    }
    if from_block > to_block {
        bail!("raw-fact normalized-event replay range start {from_block} is after end {to_block}");
    }
    Ok(())
}

async fn load_replay_raw_log_selection_for_range(
    pool: &sqlx::PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
) -> Result<ReplayRawLogSelection> {
    let canonical_raw_log_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM raw_logs AS logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
        WHERE logs.chain_id = $1
          AND logs.block_number >= $2
          AND logs.block_number <= $3
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to count canonical raw log replay inputs for chain {chain} range {from_block}..={to_block}"
        )
    })?;
    let canonical_raw_log_count = usize::try_from(canonical_raw_log_count)
        .context("canonical raw log count overflowed usize")?;

    let block_rows = sqlx::query(
        r#"
        SELECT logs.block_number, logs.block_hash
        FROM raw_logs AS logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
        WHERE logs.chain_id = $1
          AND logs.block_number >= $2
          AND logs.block_number <= $3
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        GROUP BY logs.block_number, logs.block_hash
        ORDER BY logs.block_number, logs.block_hash
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to list canonical raw log replay block hashes for chain {chain} range {from_block}..={to_block}"
        )
    })?;
    let block_hashes = block_rows
        .into_iter()
        .map(|row| row.get::<String, _>("block_hash"))
        .collect::<Vec<_>>();

    let address_rows = sqlx::query(
        r#"
        SELECT LOWER(logs.emitting_address) AS emitting_address
        FROM raw_logs AS logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
        WHERE logs.chain_id = $1
          AND logs.block_number >= $2
          AND logs.block_number <= $3
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        GROUP BY LOWER(logs.emitting_address)
        ORDER BY LOWER(logs.emitting_address)
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to list canonical raw log replay emitters for chain {chain} range {from_block}..={to_block}"
        )
    })?;
    let address_targets = address_rows
        .into_iter()
        .map(|row| (chain.to_owned(), row.get::<String, _>("emitting_address")))
        .collect::<Vec<_>>();

    Ok(ReplayRawLogSelection {
        range: Some((from_block, to_block)),
        block_hashes,
        address_targets,
        canonical_raw_log_count,
    })
}

fn replay_manifest_scope_range_for_raw_logs(
    raw_logs: &[bigname_storage::RawLogReplayInput],
) -> Result<Option<(i64, i64)>> {
    let from_block = raw_logs.iter().map(|raw_log| raw_log.block_number).min();
    let to_block = raw_logs.iter().map(|raw_log| raw_log.block_number).max();
    match (from_block, to_block) {
        (Some(from_block), Some(to_block)) => Ok(Some((from_block, to_block))),
        (None, None) => Ok(None),
        _ => bail!("raw log replay input block range is internally inconsistent"),
    }
}
