use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, RawBlock, RawCodeHash, RawLog, RawPayloadCacheMetadataUpsert, RawReceipt,
    RawTransaction, load_chain_lineage_block, load_raw_block, load_raw_blocks_by_hashes,
    load_raw_code_hash_counts_by_block_hashes, upsert_raw_blocks, upsert_raw_code_hashes,
    upsert_raw_logs, upsert_raw_payload_cache_metadata, upsert_raw_receipts,
    upsert_raw_transactions,
};
use tracing::info;

use crate::{
    provider::{ChainProviderOps, ProviderBlockSelection, ProviderHeadSnapshot},
    runtime::IntakeChainTask,
};

use super::{
    adapter_sync::sync_live_adapter_state_from_persisted_raw_payloads,
    payload::{
        canonical_raw_state, ensure_provider_bundle_matches_raw_block, insert_raw_block_candidate,
        provider_code_observation_to_raw_code_hash, provider_logs_to_live_selected_raw_logs,
        provider_raw_payload_cache_metadata_to_upserts, provider_receipts_to_selected_raw_receipts,
        provider_transactions_to_selected_raw_transactions, raw_code_hash_candidate_hashes,
        raw_payload_candidate_hashes, retained_transaction_keys_from_raw_logs,
        selected_address_set,
    },
    types::{CanonicalReconciliation, HeadChangeSet, HeaderAuditMode},
};

pub(crate) async fn persist_reconciled_raw_blocks(
    pool: &sqlx::PgPool,
    chain: &str,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    header_audit_mode: HeaderAuditMode,
) -> Result<()> {
    let mut blocks = BTreeMap::<String, bigname_storage::RawBlock>::new();

    let canonical_state = canonical_raw_state(canonical.status);
    for block in &canonical.reconciled_blocks {
        insert_raw_block_candidate(
            &mut blocks,
            chain,
            block,
            canonical_state,
            header_audit_mode,
        );
    }
    if let Some(safe) = &heads.safe {
        insert_raw_block_candidate(
            &mut blocks,
            chain,
            safe,
            CanonicalityState::Safe,
            header_audit_mode,
        );
    }
    if let Some(finalized) = &heads.finalized {
        insert_raw_block_candidate(
            &mut blocks,
            chain,
            finalized,
            CanonicalityState::Finalized,
            header_audit_mode,
        );
    }

    upsert_raw_blocks(pool, &blocks.into_values().collect::<Vec<_>>()).await?;
    Ok(())
}

pub(crate) async fn persist_reconciled_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    selected_addresses: &[String],
    provider: &(impl ChainProviderOps + ?Sized),
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
    adapter_sync_enabled: bool,
) -> Result<()> {
    let block_hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set);
    if block_hashes.is_empty() {
        return Ok(());
    }

    let raw_blocks = load_raw_blocks_by_hashes(pool, chain, &block_hashes).await?;
    if raw_blocks.len() != block_hashes.len() {
        bail!(
            "stored raw block count {} does not match the raw payload fetch plan size {} for chain {}",
            raw_blocks.len(),
            block_hashes.len(),
            chain
        );
    }

    let mut transactions = Vec::<RawTransaction>::new();
    let mut receipts = Vec::<RawReceipt>::new();
    let mut logs = Vec::<RawLog>::new();
    let mut cache_metadata = Vec::<RawPayloadCacheMetadataUpsert>::new();
    let selected_addresses = selected_address_set(selected_addresses);

    for raw_block in &raw_blocks {
        let bundle = provider
            .fetch_block_bundle_by_hash(&raw_block.block_hash)
            .await?;
        ensure_provider_bundle_matches_raw_block(raw_block, &bundle)?;

        cache_metadata.extend(provider_raw_payload_cache_metadata_to_upserts(
            chain,
            raw_block,
            &bundle.raw_payloads,
        ));
        let selected_logs = provider_logs_to_live_selected_raw_logs(
            chain,
            raw_block,
            &bundle.logs,
            &selected_addresses,
        )?;
        let retained_transaction_keys = retained_transaction_keys_from_raw_logs(&selected_logs);

        transactions.extend(provider_transactions_to_selected_raw_transactions(
            chain,
            raw_block,
            &bundle.transactions,
            &retained_transaction_keys,
        )?);
        receipts.extend(provider_receipts_to_selected_raw_receipts(
            chain,
            raw_block,
            &bundle.receipts,
            &retained_transaction_keys,
        )?);
        logs.extend(selected_logs);
    }

    upsert_raw_payload_cache_metadata(pool, &cache_metadata).await?;
    upsert_raw_transactions(pool, &transactions).await?;
    upsert_raw_receipts(pool, &receipts).await?;
    upsert_raw_logs(pool, &logs).await?;
    if adapter_sync_enabled {
        sync_live_adapter_state_from_persisted_raw_payloads(pool, chain, &block_hashes).await?;
    } else {
        info!(
            service = "indexer",
            command = "poll",
            chain,
            block_hash_count = block_hashes.len(),
            raw_log_count = logs.len(),
            "live raw payload adapter sync skipped after raw fact persistence"
        );
    }

    Ok(())
}

pub(crate) async fn ensure_losing_branch_raw_blocks_exist(
    pool: &sqlx::PgPool,
    chain: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<()> {
    if stop_before_hash == Some(from_hash) {
        return Ok(());
    }

    let mut missing_raw_blocks = Vec::<RawBlock>::new();
    let mut cursor_hash = Some(from_hash.to_owned());

    while let Some(block_hash) = cursor_hash {
        if Some(block_hash.as_str()) == stop_before_hash {
            break;
        }

        if let Some(raw_block) = load_raw_block(pool, chain, &block_hash).await? {
            cursor_hash = raw_block.parent_hash.clone();
            continue;
        }

        let lineage_block = load_chain_lineage_block(pool, chain, &block_hash)
            .await?
            .with_context(|| {
                format!(
                    "missing stored lineage row for chain {chain} block {block_hash} while materializing losing-branch raw blocks"
                )
            })?;
        cursor_hash = lineage_block.parent_hash.clone();
        missing_raw_blocks.push(RawBlock {
            chain_id: lineage_block.chain_id,
            block_hash: lineage_block.block_hash,
            parent_hash: lineage_block.parent_hash,
            block_number: lineage_block.block_number,
            block_timestamp: lineage_block.block_timestamp,
            logs_bloom: lineage_block.logs_bloom,
            transactions_root: lineage_block.transactions_root,
            receipts_root: lineage_block.receipts_root,
            state_root: lineage_block.state_root,
            canonicality_state: CanonicalityState::Orphaned,
        });
    }

    if !missing_raw_blocks.is_empty() {
        upsert_raw_blocks(pool, &missing_raw_blocks).await?;
    }

    Ok(())
}

pub(crate) async fn persist_reconciled_raw_code_hashes(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Result<()> {
    if task.addresses.is_empty() {
        return Ok(());
    }

    let refreshed_block_hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let candidate_hashes = raw_code_hash_candidate_hashes(heads, canonical, head_change_set);
    if candidate_hashes.is_empty() {
        return Ok(());
    }

    let raw_blocks = load_raw_blocks_by_hashes(pool, &task.chain, &candidate_hashes).await?;
    if raw_blocks.len() != candidate_hashes.len() {
        bail!(
            "stored raw block count {} does not match the raw code-hash fetch plan size {} for chain {}",
            raw_blocks.len(),
            candidate_hashes.len(),
            task.chain
        );
    }

    let stored_counts =
        load_raw_code_hash_counts_by_block_hashes(pool, &task.chain, &candidate_hashes).await?;
    let raw_blocks = raw_blocks
        .into_iter()
        .filter(|raw_block| {
            refreshed_block_hashes.contains(&raw_block.block_hash)
                || stored_counts
                    .get(&raw_block.block_hash)
                    .copied()
                    .unwrap_or(0)
                    < task.addresses.len()
        })
        .collect::<Vec<_>>();
    if raw_blocks.is_empty() {
        return Ok(());
    }

    let mut code_hashes = Vec::<RawCodeHash>::new();
    for raw_block in &raw_blocks {
        let observations = provider
            .fetch_code_observations_at_block(
                &task.addresses,
                ProviderBlockSelection::Hash(raw_block.block_hash.clone()),
            )
            .await?;
        code_hashes.extend(
            observations
                .iter()
                .map(|observation| {
                    provider_code_observation_to_raw_code_hash(&task.chain, raw_block, observation)
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }

    upsert_raw_code_hashes(pool, &code_hashes).await?;
    Ok(())
}
