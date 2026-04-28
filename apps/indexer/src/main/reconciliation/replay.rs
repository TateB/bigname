use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use bigname_manifests::{
    WatchedContract, WatchedSourceSelector, load_manifest_declared_watched_source_selector_plan,
    load_watched_chain_plan, load_watched_contracts_by_addresses,
};
use bigname_storage::list_canonical_raw_log_replay_inputs_for_block_hashes;
use sqlx::Row;

use super::{
    adapter_sync::sync_replay_normalized_events_from_persisted_raw_payloads,
    types::{
        PersistedRawPayloadAdapterSyncSummary, RawFactNormalizedEventReplayOutcome,
        RawFactNormalizedEventReplayRequest, RawFactNormalizedEventReplaySelection,
    },
};

pub(crate) async fn replay_raw_fact_normalized_events(
    pool: &sqlx::PgPool,
    request: RawFactNormalizedEventReplayRequest,
) -> Result<RawFactNormalizedEventReplayOutcome> {
    if request.deployment_profile.trim().is_empty() {
        bail!("deployment_profile must not be empty");
    }

    let selection_kind = request.selection.as_str();
    let raw_log_selection = load_replay_raw_log_selection(pool, &request).await?;
    ensure_replay_matches_deployment_profile_scope(pool, &request, raw_log_selection.range).await?;

    ensure_replay_block_hashes_have_only_canonical_raw_logs(
        pool,
        &request.chain,
        &raw_log_selection.block_hashes,
    )
    .await?;
    let source_scope = load_replay_adapter_source_scope(
        pool,
        &request,
        raw_log_selection.range,
        &raw_log_selection.address_targets,
    )
    .await?;

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
        )
        .await?
    };

    Ok(RawFactNormalizedEventReplayOutcome {
        deployment_profile: request.deployment_profile,
        chain: request.chain,
        selection_kind,
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
            if *from_block < 0 || *to_block < 0 {
                bail!(
                    "raw-fact normalized-event replay range must be non-negative, got {from_block}..={to_block}"
                );
            }
            if from_block > to_block {
                bail!(
                    "raw-fact normalized-event replay range start {from_block} is after end {to_block}"
                );
            }
            load_replay_raw_log_selection_for_range(pool, &request.chain, *from_block, *to_block)
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

async fn ensure_replay_matches_deployment_profile_scope(
    pool: &sqlx::PgPool,
    request: &RawFactNormalizedEventReplayRequest,
    range: Option<(i64, i64)>,
) -> Result<()> {
    let active_profile = infer_active_manifest_deployment_profile(pool).await?;
    if request.deployment_profile != active_profile {
        bail!(
            "deployment_profile {} does not match active manifest/discovery corpus profile {active_profile}",
            request.deployment_profile
        );
    }

    if let Some((from_block, to_block)) = range {
        load_manifest_declared_watched_source_selector_plan(
            pool,
            &request.chain,
            WatchedSourceSelector::WholeActiveWatchedChain,
            from_block,
            to_block,
        )
        .await
        .with_context(|| {
            format!(
                "deployment_profile {} has no active watched manifest/discovery route for chain {} over replay range {}..={}",
                request.deployment_profile, request.chain, from_block, to_block
            )
        })?;
    } else {
        ensure_active_watched_chain_for_replay_profile(
            pool,
            &request.deployment_profile,
            &request.chain,
        )
        .await?;
    }

    Ok(())
}

async fn load_replay_adapter_source_scope(
    pool: &sqlx::PgPool,
    request: &RawFactNormalizedEventReplayRequest,
    range: Option<(i64, i64)>,
    address_targets: &[(String, String)],
) -> Result<Vec<(String, String, i64, i64)>> {
    let Some((from_block, to_block)) = range else {
        return Ok(Vec::new());
    };
    if address_targets.is_empty() {
        return Ok(Vec::new());
    }

    let watched_contracts = load_watched_contracts_by_addresses(pool, &address_targets)
        .await
        .with_context(|| {
            format!(
                "failed to load replay source scope targets for chain {} range {}..={}",
                request.chain, from_block, to_block
            )
        })?;
    let source_scope = replay_source_scope_from_watched_contracts(
        &watched_contracts,
        &request.chain,
        from_block,
        to_block,
    )
    .with_context(|| {
        format!(
            "failed to build replay adapter source scope for chain {} range {}..={}",
            request.chain, from_block, to_block
        )
    })?;

    Ok(source_scope)
}

fn replay_source_scope_from_watched_contracts(
    watched_contracts: &[WatchedContract],
    chain: &str,
    from_block: i64,
    to_block: i64,
) -> Result<Vec<(String, String, i64, i64)>> {
    let mut source_scope = BTreeSet::new();
    for contract in watched_contracts {
        if contract.chain != chain {
            continue;
        }

        let effective_from_block = contract
            .active_from_block_number
            .map_or(from_block, |active_from| active_from.max(from_block));
        let effective_to_block = contract
            .active_to_block_number
            .map_or(to_block, |active_to| active_to.min(to_block));
        if effective_from_block > effective_to_block {
            continue;
        }

        source_scope.insert((
            contract.source_family.clone(),
            contract.address.to_ascii_lowercase(),
            effective_from_block,
            effective_to_block,
        ));
    }

    Ok(source_scope.into_iter().collect())
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

async fn ensure_active_watched_chain_for_replay_profile(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<()> {
    let watched_plan = load_watched_chain_plan(pool).await.with_context(|| {
        format!(
            "failed to verify deployment_profile {deployment_profile} active watched chain route for chain {chain}"
        )
    })?;
    if !watched_plan.iter().any(|plan| plan.chain == chain) {
        bail!(
            "deployment_profile {deployment_profile} has no active watched manifest/discovery route for chain {chain}"
        );
    }

    Ok(())
}

async fn infer_active_manifest_deployment_profile(pool: &sqlx::PgPool) -> Result<String> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT DISTINCT chain, deployment_epoch
        FROM manifest_versions
        WHERE rollout_status = 'active'
        ORDER BY chain, deployment_epoch
        "#,
    )
    .fetch_all(pool)
    .await
    .context(
        "failed to load active manifest/discovery corpus for replay deployment_profile enforcement",
    )?;

    if rows.is_empty() {
        bail!("deployment_profile cannot be enforced because no active manifests are loaded");
    }

    let all_mainnet = rows.iter().all(|(chain, _)| chain.ends_with("-mainnet"));
    if all_mainnet {
        return Ok("mainnet".to_owned());
    }

    let all_sepolia_dev = rows.iter().all(|(chain, deployment_epoch)| {
        chain.ends_with("-sepolia") && deployment_epoch.ends_with("_sepolia_dev")
    });
    if all_sepolia_dev {
        return Ok("sepolia-dev".to_owned());
    }

    bail!(
        "deployment_profile cannot be enforced because the active manifest/discovery corpus does not match a supported deployment profile"
    );
}

async fn ensure_replay_block_hashes_have_only_canonical_raw_logs(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<()> {
    if block_hashes.is_empty() {
        return Ok(());
    }

    let has_noncanonical_logs = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM raw_logs
            WHERE chain_id = $1
              AND block_hash = ANY($2::TEXT[])
              AND canonicality_state NOT IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to verify canonical raw log replay guard for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })?;

    if has_noncanonical_logs {
        bail!(
            "raw-fact normalized-event replay selected noncanonical raw logs; refusing block-hash-scoped adapter replay"
        );
    }

    Ok(())
}
