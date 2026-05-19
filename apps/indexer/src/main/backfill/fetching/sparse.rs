use std::{collections::BTreeMap, time::Instant};

use anyhow::{Context, Result, bail};
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    RawCodeHash, RawLog, RawReceipt, RawTransaction, upsert_chain_lineage_blocks_without_snapshots,
    upsert_raw_code_hashes, upsert_raw_logs_without_snapshots,
    upsert_raw_receipts_without_snapshots, upsert_raw_transactions_without_snapshots,
};
use tracing::info;

use crate::{
    provider::{ChainProviderOps, ProviderLog, ProviderResolvedBlock},
    reconciliation::{
        HeaderAuditMode, provider_block_to_lineage_with_header_audit_mode,
        provider_block_to_raw_block_with_header_audit_mode,
        provider_code_observation_to_raw_code_hash, provider_log_to_raw_log,
        provider_receipt_to_raw_receipt, provider_transaction_to_raw_transaction,
    },
};

use super::{
    BackfillCanonicalityEvidence, RawOnlySparseBackfillTiming,
    selected_addresses_for_materialized_block, uses_topic_first_source_family_scan,
};
use crate::backfill::selection::SelectedTargetIntervalIndex;
use crate::backfill::{BackfillAdapterSyncMode, BackfillBlockRange, BackfillOutcome};

#[path = "sparse/plan.rs"]
mod plan;

use plan::{SparseCodeObservationPlan, transaction_receipt_requests_from_raw_logs};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RawOnlySparseMaterialization {
    pub(super) range: BackfillBlockRange,
    pub(super) resolved_blocks: Vec<ProviderResolvedBlock>,
    pub(super) logs_by_block: BTreeMap<i64, Vec<ProviderLog>>,
}

pub(super) fn raw_only_sparse_materialization_slices(
    resolved_blocks: &[ProviderResolvedBlock],
    logs_by_block: &BTreeMap<i64, Vec<ProviderLog>>,
    max_raw_logs_per_push: usize,
) -> Result<Vec<RawOnlySparseMaterialization>> {
    if resolved_blocks.is_empty() {
        return Ok(Vec::new());
    }
    let max_raw_logs_per_push = max_raw_logs_per_push.max(1);
    let mut materializations = Vec::new();
    let mut start_index = 0usize;
    let mut log_count = 0usize;

    for (index, block) in resolved_blocks.iter().enumerate() {
        let block_log_count = logs_by_block
            .get(&block.block_number)
            .map(Vec::len)
            .unwrap_or(0);
        if index > start_index
            && log_count > 0
            && log_count + block_log_count > max_raw_logs_per_push
        {
            materializations.push(raw_only_sparse_materialization_slice(
                resolved_blocks,
                logs_by_block,
                start_index,
                index,
            )?);
            start_index = index;
            log_count = 0;
        }

        log_count += block_log_count;
        if log_count > max_raw_logs_per_push {
            materializations.push(raw_only_sparse_materialization_slice(
                resolved_blocks,
                logs_by_block,
                start_index,
                index + 1,
            )?);
            start_index = index + 1;
            log_count = 0;
        }
    }

    if start_index < resolved_blocks.len() {
        materializations.push(raw_only_sparse_materialization_slice(
            resolved_blocks,
            logs_by_block,
            start_index,
            resolved_blocks.len(),
        )?);
    }

    Ok(materializations)
}

fn raw_only_sparse_materialization_slice(
    resolved_blocks: &[ProviderResolvedBlock],
    logs_by_block: &BTreeMap<i64, Vec<ProviderLog>>,
    start_index: usize,
    end_index: usize,
) -> Result<RawOnlySparseMaterialization> {
    let slice = &resolved_blocks[start_index..end_index];
    let from_block = slice
        .first()
        .context("raw-only sparse materialization slice must not be empty")?
        .block_number;
    let to_block = slice
        .last()
        .context("raw-only sparse materialization slice must not be empty")?
        .block_number;
    let mut slice_logs_by_block = BTreeMap::new();
    for block in slice {
        if let Some(logs) = logs_by_block.get(&block.block_number) {
            slice_logs_by_block.insert(block.block_number, logs.clone());
        }
    }

    Ok(RawOnlySparseMaterialization {
        range: BackfillBlockRange {
            from_block,
            to_block,
        },
        resolved_blocks: slice.to_vec(),
        logs_by_block: slice_logs_by_block,
    })
}

pub(super) async fn run_split_hash_pinned_raw_only_sparse_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    provider: &(impl ChainProviderOps + ?Sized),
    range: BackfillBlockRange,
    canonicality_evidence: BackfillCanonicalityEvidence,
    materializations: Vec<RawOnlySparseMaterialization>,
    timing: RawOnlySparseBackfillTiming,
    header_audit_mode: HeaderAuditMode,
) -> Result<BackfillOutcome> {
    let mut merged = BackfillOutcome {
        chain: source_plan.watched_chain_plan.chain.clone(),
        from_block: range.from_block,
        to_block: range.to_block,
        resolved_block_count: 0,
        raw_block_count: 0,
        raw_transaction_count: 0,
        raw_receipt_count: 0,
        raw_log_count: 0,
        raw_code_hash_count: 0,
    };
    for materialization in materializations {
        let outcome = run_hash_pinned_raw_only_sparse_backfill_range(
            pool,
            source_plan,
            selected_target_index,
            provider,
            materialization.range,
            canonicality_evidence.clone(),
            materialization.resolved_blocks,
            materialization.logs_by_block,
            RawOnlySparseBackfillTiming {
                total_started: Instant::now(),
                resolve_ms: 0,
                range_logs_ms: 0,
            },
            header_audit_mode,
        )
        .await?;
        merged.resolved_block_count += outcome.resolved_block_count;
        merged.raw_block_count += outcome.raw_block_count;
        merged.raw_transaction_count += outcome.raw_transaction_count;
        merged.raw_receipt_count += outcome.raw_receipt_count;
        merged.raw_log_count += outcome.raw_log_count;
        merged.raw_code_hash_count += outcome.raw_code_hash_count;
    }
    info!(
        service = "indexer",
        command = "backfill",
        chain = %merged.chain,
        from_block = range.from_block,
        to_block = range.to_block,
        resolved_block_count = merged.resolved_block_count,
        raw_block_count = merged.raw_block_count,
        raw_transaction_count = merged.raw_transaction_count,
        raw_receipt_count = merged.raw_receipt_count,
        raw_log_count = merged.raw_log_count,
        raw_code_hash_count = merged.raw_code_hash_count,
        resolve_ms = timing.resolve_ms,
        range_logs_ms = timing.range_logs_ms,
        total_ms = timing.total_started.elapsed().as_millis(),
        "hash-pinned raw-only sparse split backfill completed"
    );

    Ok(merged)
}

pub(super) async fn run_hash_pinned_raw_only_sparse_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    provider: &(impl ChainProviderOps + ?Sized),
    range: BackfillBlockRange,
    canonicality_evidence: BackfillCanonicalityEvidence,
    resolved_blocks: Vec<ProviderResolvedBlock>,
    mut ranged_logs_by_block: BTreeMap<i64, Vec<ProviderLog>>,
    timing: RawOnlySparseBackfillTiming,
    header_audit_mode: HeaderAuditMode,
) -> Result<BackfillOutcome> {
    let watched_chain = &source_plan.watched_chain_plan;
    ranged_logs_by_block.retain(|_, logs| !logs.is_empty());
    let topic_filtered_source_family = uses_topic_first_source_family_scan(source_plan);
    let code_observation_block = None;

    let headers_started = Instant::now();
    let blocks = provider
        .fetch_block_headers_by_hashes(&resolved_blocks)
        .await
        .with_context(|| {
            format!(
                "failed to fetch hash-pinned block header batch for chain {} range {}..={}",
                watched_chain.chain, range.from_block, range.to_block
            )
        })?;
    let headers_ms = headers_started.elapsed().as_millis();

    let materialize_started = Instant::now();
    let mut transactions = Vec::<RawTransaction>::new();
    let mut receipts = Vec::<RawReceipt>::new();
    let mut logs = Vec::<RawLog>::new();
    let mut code_hashes = Vec::<RawCodeHash>::new();
    let mut raw_blocks_by_hash = BTreeMap::new();
    let mut lineage_blocks = Vec::with_capacity(resolved_blocks.len());
    let mut code_observation_plan = SparseCodeObservationPlan::default();
    let canonicality_states = canonicality_evidence
        .states_for_blocks(pool, &watched_chain.chain, provider, &blocks)
        .await?;

    for (resolved_block, block) in resolved_blocks.iter().zip(blocks.iter()) {
        if block.block_hash != resolved_block.block_hash {
            bail!(
                "provider returned block {} for requested hash {}",
                block.block_hash,
                resolved_block.block_hash
            );
        }
        if block.block_number != resolved_block.block_number {
            bail!(
                "provider resolved chain {} block number {} to hash {}, but hash-scoped fetch returned block number {}",
                watched_chain.chain,
                resolved_block.block_number,
                resolved_block.block_hash,
                block.block_number
            );
        }

        let canonicality_state = canonicality_states
            .get(&block.block_hash)
            .copied()
            .unwrap_or(bigname_storage::CanonicalityState::Observed);
        let raw_block = provider_block_to_raw_block_with_header_audit_mode(
            &watched_chain.chain,
            block,
            canonicality_state,
            header_audit_mode,
        );
        lineage_blocks.push(provider_block_to_lineage_with_header_audit_mode(
            &watched_chain.chain,
            block,
            canonicality_state,
            header_audit_mode,
        ));
        let block_logs = ranged_logs_by_block
            .remove(&resolved_block.block_number)
            .unwrap_or_default();
        let selected_addresses = selected_addresses_for_materialized_block(
            source_plan,
            selected_target_index,
            topic_filtered_source_family,
            resolved_block.block_number,
            &block_logs,
        );
        logs.extend(
            block_logs
                .iter()
                .filter(|log| selected_addresses.contains(&log.address.to_ascii_lowercase()))
                .map(|log| provider_log_to_raw_log(&watched_chain.chain, &raw_block, log))
                .collect::<Result<Vec<_>>>()?,
        );

        if Some(resolved_block.block_number) == code_observation_block
            && !selected_addresses.is_empty()
        {
            code_observation_plan.record(&raw_block, &selected_addresses);
        }

        raw_blocks_by_hash.insert(raw_block.block_hash.clone(), raw_block.clone());
    }
    if !ranged_logs_by_block.is_empty() {
        bail!("provider returned range logs for unprocessed backfill blocks");
    }
    let materialize_ms = materialize_started.elapsed().as_millis();

    let transaction_receipt_requests = transaction_receipt_requests_from_raw_logs(&logs);
    let transaction_receipts_started = Instant::now();
    let transaction_receipt_pairs = provider
        .fetch_transaction_receipt_pairs_by_hashes(&transaction_receipt_requests)
        .await
        .with_context(|| {
            format!(
                "failed to fetch selected transaction/receipt batch for chain {} range {}..={}",
                watched_chain.chain, range.from_block, range.to_block
            )
        })?;
    let transaction_receipts_ms = transaction_receipts_started.elapsed().as_millis();
    let transaction_materialize_started = Instant::now();
    for pair in transaction_receipt_pairs {
        let raw_block = raw_blocks_by_hash
            .get(&pair.transaction.block_hash)
            .with_context(|| {
                format!(
                    "provider returned selected transaction {} for unprocessed block {}",
                    pair.transaction.transaction_hash, pair.transaction.block_hash
                )
            })?;
        transactions.push(provider_transaction_to_raw_transaction(
            &watched_chain.chain,
            raw_block,
            &pair.transaction,
        )?);
        receipts.push(provider_receipt_to_raw_receipt(
            &watched_chain.chain,
            raw_block,
            &pair.receipt,
        )?);
    }
    let transaction_materialize_ms = transaction_materialize_started.elapsed().as_millis();

    let code_started = Instant::now();
    let code_observation_batches = provider
        .fetch_code_observations_at_block_hashes(&code_observation_plan.requests())
        .await
        .with_context(|| {
            format!(
                "failed to fetch hash-pinned code observation batch for chain {} range {}..={}",
                watched_chain.chain, range.from_block, range.to_block
            )
        })?;
    let code_fetch_ms = code_started.elapsed().as_millis();
    let code_materialize_started = Instant::now();
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
    let code_materialize_ms = code_materialize_started.elapsed().as_millis();

    let header_anchor_upsert_started = Instant::now();
    upsert_chain_lineage_blocks_without_snapshots(pool, &lineage_blocks).await?;
    let header_anchor_upsert_ms = header_anchor_upsert_started.elapsed().as_millis();
    let transactions_upsert_started = Instant::now();
    upsert_raw_transactions_without_snapshots(pool, &transactions).await?;
    let transactions_upsert_ms = transactions_upsert_started.elapsed().as_millis();
    let receipts_upsert_started = Instant::now();
    upsert_raw_receipts_without_snapshots(pool, &receipts).await?;
    let receipts_upsert_ms = receipts_upsert_started.elapsed().as_millis();
    let logs_upsert_started = Instant::now();
    upsert_raw_logs_without_snapshots(pool, &logs).await?;
    let logs_upsert_ms = logs_upsert_started.elapsed().as_millis();
    let code_hashes_upsert_started = Instant::now();
    upsert_raw_code_hashes(pool, &code_hashes).await?;
    let code_hashes_upsert_ms = code_hashes_upsert_started.elapsed().as_millis();
    if !logs.is_empty() {
        info!(
            service = "indexer",
            command = "backfill",
            chain = %watched_chain.chain,
            from_block = range.from_block,
            to_block = range.to_block,
            raw_log_count = logs.len(),
            adapter_sync_mode = BackfillAdapterSyncMode::RawOnly.as_str(),
            "hash-pinned backfill adapter sync skipped after raw fact persistence"
        );
    }
    info!(
        service = "indexer",
        command = "backfill",
        chain = %watched_chain.chain,
        from_block = range.from_block,
        to_block = range.to_block,
        resolved_block_count = resolved_blocks.len(),
        raw_block_count = resolved_blocks.len(),
        raw_log_count = logs.len(),
        raw_transaction_count = transactions.len(),
        resolve_ms = timing.resolve_ms,
        range_logs_ms = timing.range_logs_ms,
        headers_ms,
        materialize_ms,
        transaction_receipts_ms,
        transaction_materialize_ms,
        code_fetch_ms,
        code_materialize_ms,
        header_anchor_upsert_ms,
        transactions_upsert_ms,
        receipts_upsert_ms,
        logs_upsert_ms,
        code_hashes_upsert_ms,
        total_ms = timing.total_started.elapsed().as_millis(),
        "hash-pinned raw-only sparse backfill timing"
    );

    let outcome = BackfillOutcome {
        chain: watched_chain.chain.clone(),
        from_block: range.from_block,
        to_block: range.to_block,
        resolved_block_count: resolved_blocks.len(),
        raw_block_count: resolved_blocks.len(),
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

#[cfg(test)]
#[path = "tests/sparse.rs"]
mod tests;
