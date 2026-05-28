#[path = "historical/log_payloads.rs"]
mod log_payloads;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedSourceSelectorKind, WatchedSourceSelectorPlan};
use bigname_storage::{
    CanonicalityState, RawCodeHash, RawLog, RawReceipt, RawTransaction,
    upsert_chain_lineage_blocks, upsert_raw_code_hashes, upsert_raw_logs, upsert_raw_receipts,
    upsert_raw_transactions,
};
use tracing::info;

use crate::{
    provider::{
        ChainProviderOps, ProviderBlock, ProviderBlockCodeObservationRequest,
        ProviderResolvedBlock, ProviderTransactionReceiptRequest,
    },
    reconciliation::{
        HeaderAuditMode, provider_block_to_lineage_with_header_audit_mode,
        provider_block_to_raw_block_with_header_audit_mode,
        provider_code_observation_to_raw_code_hash, provider_logs_to_selected_raw_logs,
        provider_receipt_to_raw_receipt, provider_receipts_to_selected_raw_receipts,
        provider_transaction_to_raw_transaction,
        provider_transactions_to_selected_raw_transactions,
        retained_transaction_keys_from_raw_logs, sync_adapter_state_from_persisted_raw_payloads,
        sync_adapter_state_from_scoped_persisted_raw_payloads,
    },
    source_scope::SourceScope,
};
use log_payloads::fill_log_payloads_from_validation_provider;

use super::{
    BackfillCanonicalityEvidence,
    log_ranges::{selected_addresses_for_materialized_block, uses_topic_first_source_family_scan},
};
use crate::backfill::{
    BackfillAdapterSyncMode, BackfillBlockRange, BackfillOutcome, HistoricalLogPayload,
    selection::SelectedTargetIntervalIndex,
};

pub(crate) async fn materialize_historical_payload_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    validation_provider: &(impl ChainProviderOps + ?Sized),
    range: BackfillBlockRange,
    canonicality_evidence: BackfillCanonicalityEvidence,
    resolved_blocks: &[ProviderResolvedBlock],
    block_headers: Vec<ProviderBlock>,
    mut historical_payload: HistoricalLogPayload,
    adapter_sync_mode: BackfillAdapterSyncMode,
    header_audit_mode: HeaderAuditMode,
) -> Result<BackfillOutcome> {
    let watched_chain = &source_plan.watched_chain_plan;
    let source_scope =
        SourceScope::from_watched_source_plan(source_plan, range.from_block, range.to_block);
    let adapter_sync_scope = source_scope.adapter_sync_scope();
    let block_hashes = resolved_blocks
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    ensure_headers_match_resolved_blocks(resolved_blocks, &block_headers)?;
    if historical_payload.logs_need_validation_provider_payload {
        historical_payload.logs_by_block = fill_log_payloads_from_validation_provider(
            validation_provider,
            resolved_blocks,
            historical_payload.logs_by_block,
            &historical_payload.validation_filters,
            historical_payload.validation_mode,
        )
        .await?;
    }
    let canonicality_states = canonicality_evidence
        .states_for_blocks(
            pool,
            &watched_chain.chain,
            validation_provider,
            &block_headers,
        )
        .await?;
    let topic_filtered_source_family = uses_topic_first_source_family_scan(source_plan);
    let mut raw_blocks = Vec::with_capacity(resolved_blocks.len());
    let mut lineage_blocks = Vec::with_capacity(resolved_blocks.len());
    let mut logs = Vec::<RawLog>::new();
    let mut transactions = Vec::<RawTransaction>::new();
    let mut receipts = Vec::<RawReceipt>::new();
    let mut code_hashes = Vec::<RawCodeHash>::new();
    let mut raw_blocks_by_hash = BTreeMap::new();
    let mut code_observation_requests = Vec::new();

    for (resolved_block, block_header) in resolved_blocks.iter().zip(block_headers.iter()) {
        let canonicality_state = canonicality_states
            .get(&block_header.block_hash)
            .copied()
            .unwrap_or(CanonicalityState::Observed);
        let raw_block = provider_block_to_raw_block_with_header_audit_mode(
            &watched_chain.chain,
            block_header,
            canonicality_state,
            header_audit_mode,
        );
        lineage_blocks.push(provider_block_to_lineage_with_header_audit_mode(
            &watched_chain.chain,
            block_header,
            canonicality_state,
            header_audit_mode,
        ));

        let block_logs = historical_payload
            .logs_by_block
            .remove(&resolved_block.block_number)
            .unwrap_or_default();
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

        let source_transactions = historical_payload
            .transactions_by_block
            .remove(&resolved_block.block_number)
            .unwrap_or_default();
        let source_receipts = historical_payload
            .receipts_by_block
            .remove(&resolved_block.block_number)
            .unwrap_or_default();
        transactions.extend(provider_transactions_to_selected_raw_transactions(
            &watched_chain.chain,
            &raw_block,
            &source_transactions,
            &retained_transaction_keys,
        )?);
        receipts.extend(provider_receipts_to_selected_raw_receipts(
            &watched_chain.chain,
            &raw_block,
            &source_receipts,
            &retained_transaction_keys,
        )?);
        logs.extend(selected_logs);

        if !selected_addresses.is_empty() {
            code_observation_requests.push(ProviderBlockCodeObservationRequest {
                block_hash: raw_block.block_hash.clone(),
                addresses: selected_addresses.iter().cloned().collect(),
            });
        }
        raw_blocks_by_hash.insert(raw_block.block_hash.clone(), raw_block.clone());
        raw_blocks.push(raw_block);
    }
    ensure_no_unprocessed_payloads(&historical_payload)?;
    fill_missing_transaction_receipts(
        validation_provider,
        &watched_chain.chain,
        range,
        &raw_blocks_by_hash,
        &logs,
        &mut transactions,
        &mut receipts,
    )
    .await?;
    fetch_code_observations(
        validation_provider,
        &watched_chain.chain,
        range,
        &raw_blocks_by_hash,
        &code_observation_requests,
        &mut code_hashes,
    )
    .await?;

    upsert_chain_lineage_blocks(pool, &lineage_blocks).await?;
    upsert_raw_transactions(pool, &transactions).await?;
    upsert_raw_receipts(pool, &receipts).await?;
    upsert_raw_logs(pool, &logs).await?;
    upsert_raw_code_hashes(pool, &code_hashes).await?;
    maybe_sync_adapters(
        pool,
        source_plan,
        range,
        &block_hashes,
        &logs,
        adapter_sync_mode,
        &adapter_sync_scope,
    )
    .await?;

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
        "historical backfill range completed"
    );

    Ok(outcome)
}

fn ensure_headers_match_resolved_blocks(
    resolved_blocks: &[ProviderResolvedBlock],
    block_headers: &[ProviderBlock],
) -> Result<()> {
    if resolved_blocks.len() != block_headers.len() {
        bail!(
            "validation provider returned {} block headers for {} resolved blocks",
            block_headers.len(),
            resolved_blocks.len()
        );
    }
    for (resolved_block, block_header) in resolved_blocks.iter().zip(block_headers.iter()) {
        if block_header.block_number != resolved_block.block_number
            || !block_header
                .block_hash
                .eq_ignore_ascii_case(&resolved_block.block_hash)
        {
            bail!(
                "validation provider header {}:{} does not match resolved block {}:{}",
                block_header.block_number,
                block_header.block_hash,
                resolved_block.block_number,
                resolved_block.block_hash
            );
        }
    }

    Ok(())
}

fn ensure_no_unprocessed_payloads(historical_payload: &HistoricalLogPayload) -> Result<()> {
    if !historical_payload.logs_by_block.is_empty() {
        bail!("historical backfill source returned logs for unprocessed blocks");
    }
    if !historical_payload.transactions_by_block.is_empty() {
        bail!("historical backfill source returned transactions for unprocessed blocks");
    }
    if !historical_payload.receipts_by_block.is_empty() {
        bail!("historical backfill source returned receipts for unprocessed blocks");
    }

    Ok(())
}

async fn fill_missing_transaction_receipts(
    validation_provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    range: BackfillBlockRange,
    raw_blocks_by_hash: &BTreeMap<String, bigname_storage::RawBlock>,
    logs: &[RawLog],
    transactions: &mut Vec<RawTransaction>,
    receipts: &mut Vec<RawReceipt>,
) -> Result<()> {
    let requests =
        missing_transaction_receipt_requests_from_raw_facts(logs, transactions, receipts);
    let pairs = validation_provider
        .fetch_transaction_receipt_pairs_by_hashes(&requests)
        .await
        .with_context(|| {
            format!(
                "failed to fetch selected transaction/receipt batch from validation provider for chain {chain} range {}..={}",
                range.from_block, range.to_block
            )
        })?;
    let mut transaction_keys = transactions
        .iter()
        .map(|transaction| {
            (
                transaction.block_hash.clone(),
                transaction.transaction_hash.clone(),
                transaction.transaction_index,
            )
        })
        .collect::<BTreeSet<_>>();
    let mut receipt_keys = receipts
        .iter()
        .map(|receipt| {
            (
                receipt.block_hash.clone(),
                receipt.transaction_hash.clone(),
                receipt.transaction_index,
            )
        })
        .collect::<BTreeSet<_>>();
    for pair in pairs {
        let raw_block = raw_blocks_by_hash
            .get(&pair.transaction.block_hash)
            .with_context(|| {
                format!(
                    "validation provider returned selected transaction {} for unprocessed block {}",
                    pair.transaction.transaction_hash, pair.transaction.block_hash
                )
            })?;
        let key = (
            pair.transaction.block_hash.clone(),
            pair.transaction.transaction_hash.clone(),
            pair.transaction.transaction_index,
        );
        if transaction_keys.insert(key.clone()) {
            transactions.push(provider_transaction_to_raw_transaction(
                chain,
                raw_block,
                &pair.transaction,
            )?);
        }
        if receipt_keys.insert(key) {
            receipts.push(provider_receipt_to_raw_receipt(
                chain,
                raw_block,
                &pair.receipt,
            )?);
        }
    }

    Ok(())
}

async fn fetch_code_observations(
    validation_provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    range: BackfillBlockRange,
    raw_blocks_by_hash: &BTreeMap<String, bigname_storage::RawBlock>,
    code_observation_requests: &[ProviderBlockCodeObservationRequest],
    code_hashes: &mut Vec<RawCodeHash>,
) -> Result<()> {
    let batches = validation_provider
        .fetch_code_observations_at_block_hashes(code_observation_requests)
        .await
        .with_context(|| {
            format!(
                "failed to fetch historical code observation batch for chain {chain} range {}..={}",
                range.from_block, range.to_block
            )
        })?;
    for batch in batches {
        let raw_block = raw_blocks_by_hash.get(&batch.block_hash).with_context(|| {
            format!(
                "validation provider returned code observations for unrequested block hash {}",
                batch.block_hash
            )
        })?;
        code_hashes.extend(
            batch
                .observations
                .iter()
                .map(|observation| {
                    provider_code_observation_to_raw_code_hash(chain, raw_block, observation)
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }

    Ok(())
}

async fn maybe_sync_adapters(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    range: BackfillBlockRange,
    block_hashes: &[String],
    logs: &[RawLog],
    adapter_sync_mode: BackfillAdapterSyncMode,
    adapter_sync_scope: &[(String, String, i64, i64)],
) -> Result<()> {
    let watched_chain = &source_plan.watched_chain_plan;
    if !logs.is_empty() && adapter_sync_mode == BackfillAdapterSyncMode::Inline {
        if source_plan.selector_kind == WatchedSourceSelectorKind::WholeActiveWatchedChain {
            sync_adapter_state_from_persisted_raw_payloads(
                pool,
                &watched_chain.chain,
                block_hashes,
            )
            .await?;
        } else {
            sync_adapter_state_from_scoped_persisted_raw_payloads(
                pool,
                &watched_chain.chain,
                block_hashes,
                adapter_sync_scope,
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
            "historical backfill adapter sync skipped after raw fact persistence"
        );
    }

    Ok(())
}

fn missing_transaction_receipt_requests_from_raw_facts(
    logs: &[RawLog],
    transactions: &[RawTransaction],
    receipts: &[RawReceipt],
) -> Vec<ProviderTransactionReceiptRequest> {
    let transaction_keys = transactions
        .iter()
        .map(|transaction| {
            (
                transaction.block_hash.clone(),
                transaction.transaction_hash.clone(),
                transaction.transaction_index,
            )
        })
        .collect::<BTreeSet<_>>();
    let receipt_keys = receipts
        .iter()
        .map(|receipt| {
            (
                receipt.block_hash.clone(),
                receipt.transaction_hash.clone(),
                receipt.transaction_index,
            )
        })
        .collect::<BTreeSet<_>>();
    let mut requests = BTreeMap::<(String, String, i64), ProviderTransactionReceiptRequest>::new();
    for log in logs {
        let key = (
            log.block_hash.clone(),
            log.transaction_hash.clone(),
            log.transaction_index,
        );
        if transaction_keys.contains(&key) && receipt_keys.contains(&key) {
            continue;
        }
        requests
            .entry(key)
            .or_insert_with(|| ProviderTransactionReceiptRequest {
                transaction_hash: log.transaction_hash.clone(),
                block_hash: log.block_hash.clone(),
                block_number: log.block_number,
                transaction_index: log.transaction_index,
            });
    }

    requests.into_values().collect()
}
