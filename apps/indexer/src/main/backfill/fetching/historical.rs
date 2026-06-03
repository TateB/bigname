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
        ChainProviderOps, ProviderBlock, ProviderBlockCodeObservationRequest, ProviderLog,
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
pub(crate) use log_payloads::fill_log_payloads_from_validation_provider;

use super::{
    BackfillCanonicalityEvidence,
    log_ranges::{selected_addresses_for_materialized_block, uses_topic_first_source_family_scan},
};
use crate::backfill::{
    BackfillAdapterSyncMode, BackfillBlockRange, BackfillOutcome, HistoricalCodeObservationScope,
    HistoricalLogPayload, selection::SelectedTargetIntervalIndex,
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
    let code_observation_scope = historical_payload.code_observation_scope;
    let logs_filtered_by_selected_target_index =
        historical_payload.logs_filtered_by_selected_target_index;
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
        let selected_addresses = selected_addresses_for_payload_block(
            source_plan,
            selected_target_index,
            topic_filtered_source_family,
            logs_filtered_by_selected_target_index,
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
        let code_observation_addresses = code_observation_addresses_for_materialized_block(
            code_observation_scope,
            &selected_addresses,
            &selected_logs,
        );

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

        if !code_observation_addresses.is_empty() {
            code_observation_requests.push(ProviderBlockCodeObservationRequest {
                block_hash: raw_block.block_hash.clone(),
                addresses: code_observation_addresses,
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
    let adapter_sync_scope = adapter_sync_scope_for_materialized_range(
        source_plan,
        range,
        code_observation_scope,
        &logs,
    );
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

fn code_observation_addresses_for_materialized_block(
    scope: HistoricalCodeObservationScope,
    selected_addresses: &BTreeSet<String>,
    selected_logs: &[RawLog],
) -> Vec<String> {
    match scope {
        HistoricalCodeObservationScope::SelectedAddresses => {
            selected_addresses.iter().cloned().collect()
        }
        HistoricalCodeObservationScope::LogEmittersOnly => selected_logs
            .iter()
            .map(|log| log.emitting_address.to_ascii_lowercase())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}

fn selected_addresses_for_payload_block(
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    topic_filtered_source_family: bool,
    logs_filtered_by_selected_target_index: bool,
    block_number: i64,
    block_logs: &[ProviderLog],
) -> BTreeSet<String> {
    if logs_filtered_by_selected_target_index {
        return selected_target_index.addresses_for_logs_at_block(block_logs, block_number);
    }
    if source_plan.selector_kind == WatchedSourceSelectorKind::SourceFamily
        && source_plan.source_family.as_deref() == Some("basenames_base_registry")
    {
        return block_logs
            .iter()
            .map(|log| log.address.to_ascii_lowercase())
            .collect();
    }

    selected_addresses_for_materialized_block(
        source_plan,
        selected_target_index,
        topic_filtered_source_family,
        block_number,
        block_logs,
    )
}

fn adapter_sync_scope_for_materialized_range(
    source_plan: &WatchedSourceSelectorPlan,
    range: BackfillBlockRange,
    code_observation_scope: HistoricalCodeObservationScope,
    logs: &[RawLog],
) -> Vec<(String, String, i64, i64)> {
    if code_observation_scope == HistoricalCodeObservationScope::LogEmittersOnly
        && source_plan.selector_kind == WatchedSourceSelectorKind::SourceFamily
        && let Some(source_family) = source_plan.source_family.as_ref()
    {
        return logs
            .iter()
            .map(|log| log.emitting_address.to_ascii_lowercase())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|address| {
                (
                    source_family.clone(),
                    address,
                    range.from_block,
                    range.to_block,
                )
            })
            .collect();
    }

    SourceScope::from_watched_source_plan(source_plan, range.from_block, range.to_block)
        .adapter_sync_scope()
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

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_manifests::{
        WatchedBackfillTarget, WatchedChainPlan, WatchedSourceSelectorKind,
        WatchedSourceSelectorPlan,
    };

    fn raw_log(emitting_address: &str, log_index: i64) -> RawLog {
        RawLog {
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number: 42,
            transaction_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
            transaction_index: 0,
            log_index,
            emitting_address: emitting_address.to_owned(),
            topics: Vec::new(),
            data: Vec::new(),
            canonicality_state: CanonicalityState::Observed,
        }
    }

    fn provider_log(address: &str, block_number: i64) -> ProviderLog {
        ProviderLog {
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number,
            transaction_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
            transaction_index: 0,
            log_index: 0,
            address: address.to_owned(),
            topics: Vec::new(),
            data: "0x".to_owned(),
        }
    }

    #[test]
    fn selected_address_scope_observes_selected_addresses_even_without_logs() {
        let selected_addresses = BTreeSet::from([
            "0x0000000000000000000000000000000000000001".to_owned(),
            "0x0000000000000000000000000000000000000002".to_owned(),
        ]);

        assert_eq!(
            code_observation_addresses_for_materialized_block(
                HistoricalCodeObservationScope::SelectedAddresses,
                &selected_addresses,
                &[],
            ),
            vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x0000000000000000000000000000000000000002".to_owned(),
            ]
        );
    }

    #[test]
    fn log_emitter_scope_observes_only_unique_selected_log_emitters() {
        let selected_addresses = BTreeSet::from([
            "0x0000000000000000000000000000000000000001".to_owned(),
            "0x0000000000000000000000000000000000000002".to_owned(),
        ]);
        let logs = vec![
            raw_log("0x0000000000000000000000000000000000000002", 0),
            raw_log("0x0000000000000000000000000000000000000002", 1),
            raw_log("0x0000000000000000000000000000000000000003", 2),
        ];

        assert_eq!(
            code_observation_addresses_for_materialized_block(
                HistoricalCodeObservationScope::LogEmittersOnly,
                &selected_addresses,
                &logs,
            ),
            vec![
                "0x0000000000000000000000000000000000000002".to_owned(),
                "0x0000000000000000000000000000000000000003".to_owned(),
            ]
        );
    }

    #[test]
    fn log_emitter_payload_replays_only_materialized_log_emitters() {
        let source_plan = WatchedSourceSelectorPlan {
            chain: "base-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::SourceFamily,
            source_family: Some("basenames_base_registry".to_owned()),
            requested_watched_targets: Vec::new(),
            selected_targets: vec![WatchedBackfillTarget {
                source_family: "basenames_base_registry".to_owned(),
                contract_instance_id: sqlx::types::Uuid::nil(),
                address: "0x0000000000000000000000000000000000000001".to_owned(),
                effective_from_block: 1,
                effective_to_block: 100,
            }],
            watched_chain_plan: WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        };
        let logs = vec![
            raw_log("0x0000000000000000000000000000000000000002", 0),
            raw_log("0x0000000000000000000000000000000000000002", 1),
            raw_log("0x0000000000000000000000000000000000000003", 2),
        ];

        assert_eq!(
            adapter_sync_scope_for_materialized_range(
                &source_plan,
                BackfillBlockRange::new(10, 20).unwrap(),
                HistoricalCodeObservationScope::LogEmittersOnly,
                &logs,
            ),
            vec![
                (
                    "basenames_base_registry".to_owned(),
                    "0x0000000000000000000000000000000000000002".to_owned(),
                    10,
                    20,
                ),
                (
                    "basenames_base_registry".to_owned(),
                    "0x0000000000000000000000000000000000000003".to_owned(),
                    10,
                    20,
                ),
            ]
        );
    }

    #[test]
    fn prefiltered_payload_uses_log_addresses_without_rescanning_selected_targets() {
        let source_plan = WatchedSourceSelectorPlan {
            chain: "base-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::SourceFamily,
            source_family: Some("basenames_base_registry".to_owned()),
            requested_watched_targets: Vec::new(),
            selected_targets: vec![
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::from_u128(1),
                    address: "0x0000000000000000000000000000000000000001".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 100,
                },
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::from_u128(2),
                    address: "0x0000000000000000000000000000000000000002".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 100,
                },
            ],
            watched_chain_plan: WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        };
        let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
        let logs = vec![provider_log(
            "0x0000000000000000000000000000000000000002",
            42,
        )];

        assert_eq!(
            selected_addresses_for_payload_block(
                &source_plan,
                &selected_target_index,
                false,
                true,
                42,
                &logs,
            ),
            BTreeSet::from(["0x0000000000000000000000000000000000000002".to_owned()])
        );
    }

    #[test]
    fn basenames_registry_scan_all_materialization_uses_returned_log_emitters() {
        let source_plan = WatchedSourceSelectorPlan {
            chain: "base-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::SourceFamily,
            source_family: Some("basenames_base_registry".to_owned()),
            requested_watched_targets: Vec::new(),
            selected_targets: vec![
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::from_u128(1),
                    address: "0x0000000000000000000000000000000000000001".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 100,
                },
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::from_u128(2),
                    address: "0x0000000000000000000000000000000000000002".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 100,
                },
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::from_u128(3),
                    address: "0x0000000000000000000000000000000000000003".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 100,
                },
            ],
            watched_chain_plan: WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        };
        let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
        let logs = vec![provider_log(
            "0x0000000000000000000000000000000000000002",
            42,
        )];

        assert_eq!(
            selected_addresses_for_payload_block(
                &source_plan,
                &selected_target_index,
                false,
                false,
                42,
                &logs,
            ),
            BTreeSet::from(["0x0000000000000000000000000000000000000002".to_owned()])
        );
    }
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
