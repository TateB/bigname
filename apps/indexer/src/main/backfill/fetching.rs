use std::{collections::BTreeMap, time::Instant};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedSourceSelectorKind, WatchedSourceSelectorPlan};
use bigname_storage::{
    CanonicalityState, RawCodeHash, RawLog, RawPayloadCacheMetadataUpsert, RawReceipt,
    RawTransaction, upsert_chain_lineage_blocks, upsert_raw_code_hashes, upsert_raw_logs,
    upsert_raw_payload_cache_metadata, upsert_raw_receipts, upsert_raw_transactions,
};
use tracing::info;

use crate::{
    provider::{ChainProviderOps, ProviderBlockCodeObservationRequest, ProviderLog},
    reconciliation::{
        HeaderAuditMode, ensure_provider_bundle_matches_raw_block,
        provider_block_to_lineage_with_header_audit_mode,
        provider_block_to_raw_block_with_header_audit_mode,
        provider_code_observation_to_raw_code_hash, provider_logs_to_selected_raw_logs,
        provider_raw_payload_cache_metadata_to_upserts, provider_receipts_to_selected_raw_receipts,
        provider_transactions_to_selected_raw_transactions,
        retained_transaction_keys_from_raw_logs, sync_adapter_state_from_persisted_raw_payloads,
        sync_adapter_state_from_scoped_persisted_raw_payloads,
    },
};

#[path = "fetching/canonicality.rs"]
mod canonicality;
#[path = "fetching/historical.rs"]
mod historical;
#[path = "fetching/log_ranges.rs"]
mod log_ranges;
#[path = "fetching/sparse.rs"]
mod sparse;

use super::{
    BackfillAdapterSyncMode, BackfillBlockRange, BackfillOutcome,
    range_resolution::resolve_backfill_range,
    selection::{SelectedTargetIntervalIndex, selected_target_addresses_at_block},
};
use crate::source_scope::SourceScope;

pub(super) use canonicality::{BackfillCanonicalityEvidence, load_backfill_canonicality_evidence};
pub(crate) use historical::materialize_historical_payload_range;
use log_ranges::{
    fetch_backfill_logs_by_safe_ranges, selected_addresses_for_materialized_block,
    uses_topic_first_source_family_scan,
};
use sparse::{
    raw_only_sparse_materialization_slices, run_hash_pinned_raw_only_sparse_backfill_range,
    run_split_hash_pinned_raw_only_sparse_backfill_range,
};

const DEFAULT_RAW_ONLY_SPARSE_MAX_LOGS_PER_PUSH: usize = 10_000;

pub(crate) async fn run_hash_pinned_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    selected_target_addresses_for_chunk: &[String],
    provider: &(impl ChainProviderOps + ?Sized),
    range: BackfillBlockRange,
    canonicality_evidence: BackfillCanonicalityEvidence,
    adapter_sync_mode: BackfillAdapterSyncMode,
    header_audit_mode: HeaderAuditMode,
) -> Result<BackfillOutcome> {
    let watched_chain = &source_plan.watched_chain_plan;
    let source_scope =
        SourceScope::from_watched_source_plan(source_plan, range.from_block, range.to_block);
    let adapter_sync_scope = source_scope.adapter_sync_scope();
    let total_started = Instant::now();
    let resolve_started = Instant::now();
    let resolved_blocks = resolve_backfill_range(provider, range).await?;
    let resolve_ms = resolve_started.elapsed().as_millis();
    let block_hashes = resolved_blocks
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let topic_filtered_source_family = uses_topic_first_source_family_scan(source_plan);
    let fetch_logs_by_safe_ranges = resolved_blocks.len() > 1 || topic_filtered_source_family;
    let range_logs_started = Instant::now();
    let mut ranged_logs_by_block = if fetch_logs_by_safe_ranges {
        fetch_backfill_logs_by_safe_ranges(
            provider,
            source_plan,
            selected_target_index,
            selected_target_addresses_for_chunk,
            &resolved_blocks,
            range,
        )
        .await?
    } else {
        BTreeMap::new()
    };
    let range_logs_ms = range_logs_started.elapsed().as_millis();
    if adapter_sync_mode == BackfillAdapterSyncMode::RawOnly && fetch_logs_by_safe_ranges {
        let max_raw_logs_per_push = raw_only_sparse_max_logs_per_push_from_env();
        let materializations = raw_only_sparse_materialization_slices(
            &resolved_blocks,
            &ranged_logs_by_block,
            max_raw_logs_per_push,
        )?;
        if materializations.len() > 1 {
            let raw_log_count = count_provider_logs(&ranged_logs_by_block);
            info!(
                service = "indexer",
                command = "backfill",
                chain = %watched_chain.chain,
                from_block = range.from_block,
                to_block = range.to_block,
                raw_log_count,
                max_raw_logs_per_push,
                materialization_count = materializations.len(),
                "splitting hash-pinned raw-only sparse backfill pushes by selected log count"
            );
            return run_split_hash_pinned_raw_only_sparse_backfill_range(
                pool,
                source_plan,
                selected_target_index,
                provider,
                range,
                canonicality_evidence,
                materializations,
                RawOnlySparseBackfillTiming {
                    total_started,
                    resolve_ms,
                    range_logs_ms,
                },
                header_audit_mode,
            )
            .await;
        }
        let materialization = materializations
            .into_iter()
            .next()
            .context("raw-only sparse materialization must include the requested range")?;
        return run_hash_pinned_raw_only_sparse_backfill_range(
            pool,
            source_plan,
            selected_target_index,
            provider,
            materialization.range,
            canonicality_evidence,
            materialization.resolved_blocks,
            materialization.logs_by_block,
            RawOnlySparseBackfillTiming {
                total_started,
                resolve_ms,
                range_logs_ms,
            },
            header_audit_mode,
        )
        .await;
    }
    let single_block_needs_bundle_logs = resolved_blocks
        .first()
        .map(|block| {
            !selected_target_addresses_at_block(source_plan, block.block_number).is_empty()
        })
        .unwrap_or(false);
    let bundles = if fetch_logs_by_safe_ranges || !single_block_needs_bundle_logs {
        provider
            .fetch_block_bundles_without_logs_by_hashes(&resolved_blocks)
            .await
    } else {
        provider
            .fetch_block_bundles_by_hashes(&resolved_blocks)
            .await
    }
    .with_context(|| {
        format!(
            "failed to fetch hash-pinned payload batch for chain {} range {}..={}",
            watched_chain.chain, range.from_block, range.to_block
        )
    })?;
    let mut raw_blocks = Vec::with_capacity(resolved_blocks.len());
    let mut transactions = Vec::<RawTransaction>::new();
    let mut receipts = Vec::<RawReceipt>::new();
    let mut logs = Vec::<RawLog>::new();
    let mut code_hashes = Vec::<RawCodeHash>::new();
    let mut cache_metadata = Vec::<RawPayloadCacheMetadataUpsert>::new();
    let mut raw_blocks_by_hash = BTreeMap::new();
    let mut lineage_blocks = Vec::with_capacity(resolved_blocks.len());
    let mut code_observation_requests = Vec::new();
    let bundle_blocks = bundles
        .iter()
        .map(|bundle| bundle.block.clone())
        .collect::<Vec<_>>();
    let canonicality_states = canonicality_evidence
        .states_for_blocks(pool, &watched_chain.chain, provider, &bundle_blocks)
        .await?;

    for (resolved_block, bundle) in resolved_blocks.iter().zip(bundles.iter()) {
        if bundle.block.block_number != resolved_block.block_number {
            bail!(
                "provider resolved chain {} block number {} to hash {}, but hash-scoped fetch returned block number {}",
                watched_chain.chain,
                resolved_block.block_number,
                resolved_block.block_hash,
                bundle.block.block_number
            );
        }

        let canonicality_state = canonicality_states
            .get(&bundle.block.block_hash)
            .copied()
            .unwrap_or(CanonicalityState::Observed);
        let raw_block = provider_block_to_raw_block_with_header_audit_mode(
            &watched_chain.chain,
            &bundle.block,
            canonicality_state,
            header_audit_mode,
        );
        ensure_provider_bundle_matches_raw_block(&raw_block, bundle)?;

        lineage_blocks.push(provider_block_to_lineage_with_header_audit_mode(
            &watched_chain.chain,
            &bundle.block,
            canonicality_state,
            header_audit_mode,
        ));
        let block_logs = if fetch_logs_by_safe_ranges {
            ranged_logs_by_block
                .remove(&resolved_block.block_number)
                .unwrap_or_default()
        } else {
            bundle.logs.clone()
        };
        let selected_addresses = selected_addresses_for_materialized_block(
            source_plan,
            selected_target_index,
            topic_filtered_source_family,
            resolved_block.block_number,
            &block_logs,
        );
        let selected_logs = provider_logs_to_selected_raw_logs(
            &watched_chain.chain,
            &raw_block,
            &block_logs,
            &selected_addresses,
        )?;
        let retained_transaction_keys = retained_transaction_keys_from_raw_logs(&selected_logs);
        if !selected_logs.is_empty() {
            cache_metadata.extend(provider_raw_payload_cache_metadata_to_upserts(
                &watched_chain.chain,
                &raw_block,
                &bundle.raw_payloads,
            ));
        }
        transactions.extend(provider_transactions_to_selected_raw_transactions(
            &watched_chain.chain,
            &raw_block,
            &bundle.transactions,
            &retained_transaction_keys,
        )?);
        receipts.extend(provider_receipts_to_selected_raw_receipts(
            &watched_chain.chain,
            &raw_block,
            &bundle.receipts,
            &retained_transaction_keys,
        )?);
        logs.extend(selected_logs);

        if !selected_addresses.is_empty() {
            code_observation_requests.push(ProviderBlockCodeObservationRequest {
                block_hash: raw_block.block_hash.clone(),
                addresses: selected_addresses.iter().cloned().collect(),
            });
            raw_blocks_by_hash.insert(raw_block.block_hash.clone(), raw_block.clone());
        }

        raw_blocks.push(raw_block);
    }
    if !ranged_logs_by_block.is_empty() {
        bail!("provider returned range logs for unprocessed backfill blocks");
    }

    let code_observation_batches = provider
        .fetch_code_observations_at_block_hashes(&code_observation_requests)
        .await
        .with_context(|| {
            format!(
                "failed to fetch hash-pinned code observation batch for chain {} range {}..={}",
                watched_chain.chain, range.from_block, range.to_block
            )
        })?;
    for batch in code_observation_batches {
        let raw_block = raw_blocks_by_hash.get(&batch.block_hash).with_context(|| {
            format!(
                "provider returned code observations for unrequested block hash {}",
                batch.block_hash
            )
        })?;
        code_hashes.extend(
            batch
                .observations
                .iter()
                .map(|observation| {
                    provider_code_observation_to_raw_code_hash(
                        &watched_chain.chain,
                        raw_block,
                        observation,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }

    upsert_chain_lineage_blocks(pool, &lineage_blocks).await?;
    upsert_raw_payload_cache_metadata(pool, &cache_metadata).await?;
    upsert_raw_transactions(pool, &transactions).await?;
    upsert_raw_receipts(pool, &receipts).await?;
    upsert_raw_logs(pool, &logs).await?;
    upsert_raw_code_hashes(pool, &code_hashes).await?;
    if !logs.is_empty() && adapter_sync_mode == BackfillAdapterSyncMode::Inline {
        if source_plan.selector_kind == WatchedSourceSelectorKind::WholeActiveWatchedChain {
            sync_adapter_state_from_persisted_raw_payloads(
                pool,
                &watched_chain.chain,
                &block_hashes,
            )
            .await?;
        } else {
            sync_adapter_state_from_scoped_persisted_raw_payloads(
                pool,
                &watched_chain.chain,
                &block_hashes,
                &adapter_sync_scope,
            )
            .await?;
        }
    } else if !logs.is_empty() {
        info!(
            service = "indexer",
            command = "backfill",
            chain = %watched_chain.chain,
            from_block = range.from_block,
            to_block = range.to_block,
            raw_log_count = logs.len(),
            adapter_sync_mode = adapter_sync_mode.as_str(),
            "hash-pinned backfill adapter sync skipped after raw fact persistence"
        );
    }

    let outcome = BackfillOutcome {
        chain: watched_chain.chain.clone(),
        from_block: range.from_block,
        to_block: range.to_block,
        resolved_block_count: resolved_blocks.len(),
        raw_block_count: raw_blocks.len(),
        raw_transaction_count: transactions.len(),
        raw_receipt_count: receipts.len(),
        raw_log_count: logs.len(),
        raw_code_hash_count: code_hashes.len(),
    };
    info!(
        service = "indexer",
        command = "backfill",
        chain = %outcome.chain,
        from_block = outcome.from_block,
        to_block = outcome.to_block,
        resolved_block_count = outcome.resolved_block_count,
        raw_block_count = outcome.raw_block_count,
        raw_transaction_count = outcome.raw_transaction_count,
        raw_receipt_count = outcome.raw_receipt_count,
        raw_log_count = outcome.raw_log_count,
        raw_code_hash_count = outcome.raw_code_hash_count,
        "hash-pinned backfill range completed"
    );

    Ok(outcome)
}

struct RawOnlySparseBackfillTiming {
    total_started: Instant,
    resolve_ms: u128,
    range_logs_ms: u128,
}

fn count_provider_logs(logs_by_block: &BTreeMap<i64, Vec<ProviderLog>>) -> usize {
    logs_by_block.values().map(Vec::len).sum()
}

fn raw_only_sparse_max_logs_per_push_from_env() -> usize {
    std::env::var("BIGNAME_INDEXER_HASH_PINNED_BACKFILL_MAX_LOGS_PER_PUSH")
        .or_else(|_| std::env::var("BIGNAME_INDEXER_HASH_PINNED_BACKFILL_MAX_LOGS_PER_RANGE"))
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_RAW_ONLY_SPARSE_MAX_LOGS_PER_PUSH)
}
