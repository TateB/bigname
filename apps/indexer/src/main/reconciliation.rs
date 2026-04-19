use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, ChainCheckpoint, ChainCheckpointUpdate, ChainLineageBlock,
    CheckpointBlockRef, RawBlock, RawCodeHash, RawLog, RawReceipt, RawTransaction,
    advance_chain_checkpoints, load_chain_lineage_block, load_raw_block, load_raw_blocks_by_hashes,
    load_raw_code_hash_counts_by_block_hashes, mark_block_derived_normalized_events_range_orphaned,
    mark_chain_lineage_range_orphaned, mark_identity_rows_range_orphaned,
    mark_raw_block_facts_range_orphaned, upsert_chain_lineage_blocks, upsert_raw_blocks,
    upsert_raw_code_hashes, upsert_raw_logs, upsert_raw_receipts, upsert_raw_transactions,
};
use sha3::{Digest, Keccak256};
use tracing::{info, warn};

use crate::{
    MAX_PARENT_FETCH_DEPTH,
    provider::{
        self, ProviderBlock, ProviderBlockBundle, ProviderBlockSelection, ProviderCodeObservation,
        ProviderHeadSnapshot, ProviderLog, ProviderReceipt, ProviderRegistry, ProviderTransaction,
    },
    runtime::{
        IntakeChainTask, checkpoint_mode, log_block_derived_normalized_event_summary,
        log_ens_v1_reverse_claim_sync_summary, log_ens_v1_unwrapped_authority_sync_summary,
    },
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CanonicalReconciliationStatus {
    Initialized,
    Unchanged,
    Appended,
    GapBackfilled,
    ReorgReconciled,
    AwaitingAncestor,
}

impl CanonicalReconciliationStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Initialized => "initialized",
            Self::Unchanged => "unchanged",
            Self::Appended => "appended",
            Self::GapBackfilled => "gap_backfilled",
            Self::ReorgReconciled => "reorg_reconciled",
            Self::AwaitingAncestor => "awaiting_ancestor",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalReconciliation {
    pub(crate) status: CanonicalReconciliationStatus,
    pub(crate) canonical: Option<CheckpointBlockRef>,
    pub(crate) fetched_parent_count: usize,
    pub(crate) orphaned_block_count: usize,
    pub(crate) reconciled_blocks: Vec<ProviderBlock>,
    pub(crate) raw_orphan_stop_before_hash: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HeadChangeSet {
    pub(crate) canonical_head_changed: bool,
    pub(crate) safe_head_changed: bool,
    pub(crate) finalized_head_changed: bool,
}

impl HeadChangeSet {
    fn requires_raw_payload_refresh(self, canonical_status: CanonicalReconciliationStatus) -> bool {
        canonical_status != CanonicalReconciliationStatus::Unchanged
            || self.safe_head_changed
            || self.finalized_head_changed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChainReconciliationOutcome {
    pub(crate) chain: String,
    pub(crate) canonical_status: CanonicalReconciliationStatus,
    pub(crate) canonical_head_changed: bool,
    pub(crate) safe_head_changed: bool,
    pub(crate) finalized_head_changed: bool,
    pub(crate) fetched_parent_count: usize,
    pub(crate) orphaned_block_count: usize,
    pub(crate) canonical_block_number: Option<i64>,
    pub(crate) safe_block_number: Option<i64>,
    pub(crate) finalized_block_number: Option<i64>,
}

pub(crate) fn log_chain_reconciliation_outcome(outcome: &ChainReconciliationOutcome) {
    info!(
        service = "indexer",
        chain = %outcome.chain,
        canonical_reconciliation_status = outcome.canonical_status.as_str(),
        canonical_head_changed = outcome.canonical_head_changed,
        safe_head_changed = outcome.safe_head_changed,
        finalized_head_changed = outcome.finalized_head_changed,
        fetched_parent_count = outcome.fetched_parent_count,
        orphaned_block_count = outcome.orphaned_block_count,
        canonical_block_number = outcome.canonical_block_number,
        safe_block_number = outcome.safe_block_number,
        finalized_block_number = outcome.finalized_block_number,
        "provider heads reconciled for chain"
    );
}

pub(crate) async fn poll_provider_heads(
    pool: &sqlx::PgPool,
    tasks: &mut Vec<IntakeChainTask>,
    provider_registry: &ProviderRegistry,
) -> Result<()> {
    let mut next_tasks = tasks.clone();
    let mut any_change = false;

    for (index, task) in tasks.iter().enumerate() {
        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            continue;
        };

        match reconcile_intake_chain_task(pool, task, provider).await {
            Ok(Some((next_task, outcome))) => {
                log_chain_reconciliation_outcome(&outcome);
                next_tasks[index] = next_task;
                any_change = true;
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    service = "indexer",
                    chain = %task.chain,
                    error = ?error,
                    intake_checkpoint_mode = checkpoint_mode(&task.checkpoint),
                    "failed to fetch and reconcile provider heads for intake chain"
                );
            }
        }
    }

    if any_change {
        *tasks = next_tasks;
    }

    Ok(())
}

pub(crate) async fn reconcile_intake_chain_task(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &provider::JsonRpcProvider,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    let heads = provider.fetch_chain_heads().await?;
    reconcile_fetched_heads(pool, task, provider, &heads).await
}

pub(crate) async fn reconcile_fetched_heads(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &provider::JsonRpcProvider,
    heads: &ProviderHeadSnapshot,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    let canonical = reconcile_canonical_head(
        pool,
        provider,
        &task.chain,
        &task.checkpoint,
        &heads.canonical,
    )
    .await?;
    let head_change_set = head_change_set(task, heads, &canonical);

    if canonical.status == CanonicalReconciliationStatus::ReorgReconciled
        && let Some(current_canonical_hash) = task.checkpoint.canonical_block_hash.as_deref()
        && load_raw_block(pool, &task.chain, current_canonical_hash)
            .await?
            .is_some()
    {
        mark_raw_block_facts_range_orphaned(
            pool,
            &task.chain,
            current_canonical_hash,
            canonical.raw_orphan_stop_before_hash.as_deref(),
        )
        .await?;
        let orphaned_normalized_event_count = mark_block_derived_normalized_events_range_orphaned(
            pool,
            &task.chain,
            current_canonical_hash,
            canonical.raw_orphan_stop_before_hash.as_deref(),
        )
        .await?;
        if orphaned_normalized_event_count > 0 {
            info!(
                service = "indexer",
                chain = %task.chain,
                orphaned_normalized_event_count,
                "block-derived normalized events orphaned for the losing branch"
            );
        }
        let orphaned_identity_counts = mark_identity_rows_range_orphaned(
            pool,
            &task.chain,
            current_canonical_hash,
            canonical.raw_orphan_stop_before_hash.as_deref(),
        )
        .await?;
        if orphaned_identity_counts.token_lineage_count > 0
            || orphaned_identity_counts.resource_count > 0
            || orphaned_identity_counts.name_surface_count > 0
            || orphaned_identity_counts.surface_binding_count > 0
        {
            info!(
                service = "indexer",
                chain = %task.chain,
                orphaned_token_lineage_count = orphaned_identity_counts.token_lineage_count,
                orphaned_resource_count = orphaned_identity_counts.resource_count,
                orphaned_name_surface_count = orphaned_identity_counts.name_surface_count,
                orphaned_surface_binding_count = orphaned_identity_counts.surface_binding_count,
                "identity rows orphaned for the losing branch"
            );
        }
    }

    persist_reconciled_raw_blocks(pool, &task.chain, heads, &canonical).await?;
    if head_change_set.requires_raw_payload_refresh(canonical.status) {
        persist_reconciled_raw_payloads(
            pool,
            &task.chain,
            provider,
            heads,
            &canonical,
            head_change_set,
        )
        .await?;
    }
    persist_reconciled_raw_code_hashes(pool, task, provider, heads, &canonical, head_change_set)
        .await?;

    if let Some(safe_head) = &heads.safe {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                &task.chain,
                safe_head,
                CanonicalityState::Safe,
            )],
        )
        .await?;
    }
    if let Some(finalized_head) = &heads.finalized {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                &task.chain,
                finalized_head,
                CanonicalityState::Finalized,
            )],
        )
        .await?;
    }

    let next_checkpoint = advance_chain_checkpoints(
        pool,
        &ChainCheckpointUpdate {
            chain_id: task.chain.clone(),
            canonical: canonical.canonical.clone(),
            safe: heads.safe.as_ref().map(provider_block_to_checkpoint_ref),
            finalized: heads
                .finalized
                .as_ref()
                .map(provider_block_to_checkpoint_ref),
        },
    )
    .await?;

    if !head_change_set.canonical_head_changed
        && !head_change_set.safe_head_changed
        && !head_change_set.finalized_head_changed
        && canonical.status == CanonicalReconciliationStatus::Unchanged
    {
        return Ok(None);
    }

    let mut next_task = task.clone();
    next_task.checkpoint = next_checkpoint.clone();

    Ok(Some((
        next_task,
        ChainReconciliationOutcome {
            chain: task.chain.clone(),
            canonical_status: canonical.status,
            canonical_head_changed: head_change_set.canonical_head_changed,
            safe_head_changed: head_change_set.safe_head_changed,
            finalized_head_changed: head_change_set.finalized_head_changed,
            fetched_parent_count: canonical.fetched_parent_count,
            orphaned_block_count: canonical.orphaned_block_count,
            canonical_block_number: next_checkpoint.canonical_block_number,
            safe_block_number: next_checkpoint.safe_block_number,
            finalized_block_number: next_checkpoint.finalized_block_number,
        },
    )))
}

pub(crate) async fn reconcile_canonical_head(
    pool: &sqlx::PgPool,
    provider: &provider::JsonRpcProvider,
    chain: &str,
    checkpoint: &ChainCheckpoint,
    latest_head: &ProviderBlock,
) -> Result<CanonicalReconciliation> {
    let latest_hash = latest_head.block_hash.as_str();
    let current_canonical_hash = checkpoint.canonical_block_hash.as_deref();

    if current_canonical_hash.is_none() {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                chain,
                latest_head,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
        return Ok(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::Initialized,
            canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
            fetched_parent_count: 0,
            orphaned_block_count: 0,
            reconciled_blocks: vec![latest_head.clone()],
            raw_orphan_stop_before_hash: None,
        });
    }

    if current_canonical_hash == Some(latest_hash) {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                chain,
                latest_head,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
        return Ok(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::Unchanged,
            canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
            fetched_parent_count: 0,
            orphaned_block_count: 0,
            reconciled_blocks: vec![latest_head.clone()],
            raw_orphan_stop_before_hash: None,
        });
    }

    let mut path = vec![latest_head.clone()];
    let mut cursor = latest_head.clone();
    let mut fetched_parent_count = 0usize;
    let mut common_ancestor_hash = None::<String>;

    for _ in 0..MAX_PARENT_FETCH_DEPTH {
        if cursor.parent_hash.as_deref() == current_canonical_hash {
            common_ancestor_hash = current_canonical_hash.map(ToOwned::to_owned);
            break;
        }

        let Some(parent_hash) = cursor.parent_hash.clone() else {
            break;
        };

        if let Some(stored_parent) = load_chain_lineage_block(pool, chain, &parent_hash).await? {
            if stored_parent.canonicality_state != CanonicalityState::Orphaned {
                common_ancestor_hash = Some(stored_parent.block_hash.clone());
                break;
            }

            cursor = lineage_block_to_provider(&stored_parent);
            path.push(cursor.clone());
            continue;
        }

        let fetched_parent = provider.fetch_block_by_hash(&parent_hash).await?;
        fetched_parent_count += 1;
        if Some(fetched_parent.block_hash.as_str()) == current_canonical_hash {
            common_ancestor_hash = Some(fetched_parent.block_hash.clone());
            break;
        }

        cursor = fetched_parent.clone();
        path.push(fetched_parent);
    }

    if common_ancestor_hash.is_none() {
        for block in &path {
            upsert_chain_lineage_blocks(
                pool,
                &[provider_block_to_lineage(
                    chain,
                    block,
                    CanonicalityState::Observed,
                )],
            )
            .await?;
        }

        return Ok(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::AwaitingAncestor,
            canonical: None,
            fetched_parent_count,
            orphaned_block_count: 0,
            reconciled_blocks: path,
            raw_orphan_stop_before_hash: None,
        });
    }

    let common_ancestor_hash = common_ancestor_hash.expect("checked above");
    let mut orphaned_block_count = 0usize;
    let status = if Some(common_ancestor_hash.as_str()) == current_canonical_hash {
        if path.len() == 1 {
            CanonicalReconciliationStatus::Appended
        } else {
            CanonicalReconciliationStatus::GapBackfilled
        }
    } else {
        orphaned_block_count = orphan_canonical_branch(
            pool,
            chain,
            current_canonical_hash.expect("current checkpoint must exist"),
            Some(common_ancestor_hash.as_str()),
        )
        .await?;
        CanonicalReconciliationStatus::ReorgReconciled
    };

    for block in path.iter().rev() {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                chain,
                block,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
    }

    Ok(CanonicalReconciliation {
        status,
        canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
        fetched_parent_count,
        orphaned_block_count,
        reconciled_blocks: path,
        raw_orphan_stop_before_hash: (status == CanonicalReconciliationStatus::ReorgReconciled)
            .then_some(common_ancestor_hash),
    })
}

pub(crate) async fn orphan_canonical_branch(
    pool: &sqlx::PgPool,
    chain: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<usize> {
    let mut orphaned_block_count = 0usize;
    let mut cursor_hash = Some(from_hash.to_owned());

    while let Some(block_hash) = cursor_hash {
        if Some(block_hash.as_str()) == stop_before_hash {
            break;
        }

        let snapshots =
            mark_chain_lineage_range_orphaned(pool, chain, &block_hash, stop_before_hash).await?;
        orphaned_block_count += snapshots.len();
        cursor_hash = None;
    }

    Ok(orphaned_block_count)
}

pub(crate) fn provider_block_to_lineage(
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: chain.to_owned(),
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
            block.block_timestamp_unix_secs,
        )
        .expect("provider block timestamp must fit in OffsetDateTime"),
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
        canonicality_state,
    }
}

pub(crate) fn lineage_block_to_provider(block: &ChainLineageBlock) -> ProviderBlock {
    ProviderBlock {
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp_unix_secs: block.block_timestamp.unix_timestamp(),
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
    }
}

pub(crate) fn provider_block_to_checkpoint_ref(block: &ProviderBlock) -> CheckpointBlockRef {
    CheckpointBlockRef {
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
    }
}

pub(crate) fn head_change_set(
    task: &IntakeChainTask,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
) -> HeadChangeSet {
    let next_safe = heads.safe.as_ref().map(provider_block_to_checkpoint_ref);
    let next_finalized = heads
        .finalized
        .as_ref()
        .map(provider_block_to_checkpoint_ref);

    HeadChangeSet {
        canonical_head_changed: checkpoint_ref_changed(
            task.checkpoint.canonical_block_hash.as_deref(),
            task.checkpoint.canonical_block_number,
            canonical.canonical.as_ref(),
        ),
        safe_head_changed: checkpoint_ref_changed(
            task.checkpoint.safe_block_hash.as_deref(),
            task.checkpoint.safe_block_number,
            next_safe.as_ref(),
        ),
        finalized_head_changed: checkpoint_ref_changed(
            task.checkpoint.finalized_block_hash.as_deref(),
            task.checkpoint.finalized_block_number,
            next_finalized.as_ref(),
        ),
    }
}

pub(crate) fn checkpoint_ref_changed(
    current_hash: Option<&str>,
    current_number: Option<i64>,
    next: Option<&CheckpointBlockRef>,
) -> bool {
    let Some(next) = next else {
        return false;
    };

    current_hash != Some(next.block_hash.as_str()) || current_number != Some(next.block_number)
}

pub(crate) async fn persist_reconciled_raw_blocks(
    pool: &sqlx::PgPool,
    chain: &str,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
) -> Result<()> {
    let mut blocks = std::collections::BTreeMap::<String, bigname_storage::RawBlock>::new();

    let canonical_state = canonical_raw_state(canonical.status);
    for block in &canonical.reconciled_blocks {
        insert_raw_block_candidate(&mut blocks, chain, block, canonical_state);
    }
    if let Some(safe) = &heads.safe {
        insert_raw_block_candidate(&mut blocks, chain, safe, CanonicalityState::Safe);
    }
    if let Some(finalized) = &heads.finalized {
        insert_raw_block_candidate(&mut blocks, chain, finalized, CanonicalityState::Finalized);
    }

    upsert_raw_blocks(pool, &blocks.into_values().collect::<Vec<_>>()).await?;
    Ok(())
}

pub(crate) async fn persist_reconciled_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    provider: &provider::JsonRpcProvider,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
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

    for raw_block in &raw_blocks {
        let bundle = provider
            .fetch_block_bundle_by_hash(&raw_block.block_hash)
            .await?;
        ensure_provider_bundle_matches_raw_block(raw_block, &bundle)?;

        transactions.extend(
            bundle
                .transactions
                .iter()
                .map(|transaction| {
                    provider_transaction_to_raw_transaction(chain, raw_block, transaction)
                })
                .collect::<Result<Vec<_>>>()?,
        );
        receipts.extend(
            bundle
                .receipts
                .iter()
                .map(|receipt| provider_receipt_to_raw_receipt(chain, raw_block, receipt))
                .collect::<Result<Vec<_>>>()?,
        );
        logs.extend(
            bundle
                .logs
                .iter()
                .map(|log| provider_log_to_raw_log(chain, raw_block, log))
                .collect::<Result<Vec<_>>>()?,
        );
    }

    upsert_raw_transactions(pool, &transactions).await?;
    upsert_raw_receipts(pool, &receipts).await?;
    upsert_raw_logs(pool, &logs).await?;
    let normalized_event_summary =
        bigname_adapters::sync_block_derived_normalized_events(pool, chain, &block_hashes).await?;
    log_block_derived_normalized_event_summary(chain, &normalized_event_summary);
    let reverse_claim_summary = bigname_adapters::sync_ens_v1_reverse_claim(pool, chain).await?;
    log_ens_v1_reverse_claim_sync_summary(chain, &reverse_claim_summary);
    let unwrapped_authority_summary =
        bigname_adapters::sync_ens_v1_unwrapped_authority(pool, chain).await?;
    log_ens_v1_unwrapped_authority_sync_summary(chain, &unwrapped_authority_summary);

    Ok(())
}

pub(crate) async fn persist_reconciled_raw_code_hashes(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &provider::JsonRpcProvider,
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
                ProviderBlockSelection::Number(raw_block.block_number),
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

pub(crate) fn raw_payload_candidate_hashes(
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Vec<String> {
    let mut hashes = BTreeSet::new();

    for block in &canonical.reconciled_blocks {
        hashes.insert(block.block_hash.clone());
    }

    if head_change_set.safe_head_changed
        || canonical.status == CanonicalReconciliationStatus::Initialized
    {
        if let Some(safe) = &heads.safe {
            hashes.insert(safe.block_hash.clone());
        }
    }

    if head_change_set.finalized_head_changed
        || canonical.status == CanonicalReconciliationStatus::Initialized
    {
        if let Some(finalized) = &heads.finalized {
            hashes.insert(finalized.block_hash.clone());
        }
    }

    hashes.into_iter().collect()
}

pub(crate) fn raw_code_hash_candidate_hashes(
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Vec<String> {
    let mut hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set)
        .into_iter()
        .collect::<BTreeSet<_>>();

    if let Some(canonical) = canonical.canonical.as_ref() {
        hashes.insert(canonical.block_hash.clone());
    }
    if let Some(safe) = &heads.safe {
        hashes.insert(safe.block_hash.clone());
    }
    if let Some(finalized) = &heads.finalized {
        hashes.insert(finalized.block_hash.clone());
    }

    hashes.into_iter().collect()
}

pub(crate) fn ensure_provider_bundle_matches_raw_block(
    raw_block: &RawBlock,
    bundle: &ProviderBlockBundle,
) -> Result<()> {
    let candidate = provider_block_to_raw_block(
        raw_block.chain_id.as_str(),
        &bundle.block,
        raw_block.canonicality_state,
    );

    if candidate.block_hash != raw_block.block_hash
        || candidate.parent_hash != raw_block.parent_hash
        || candidate.block_number != raw_block.block_number
        || candidate.block_timestamp != raw_block.block_timestamp
        || candidate.logs_bloom != raw_block.logs_bloom
        || candidate.transactions_root != raw_block.transactions_root
        || candidate.receipts_root != raw_block.receipts_root
        || candidate.state_root != raw_block.state_root
    {
        bail!(
            "provider bundle block {} does not match stored raw block facts for chain {}",
            raw_block.block_hash,
            raw_block.chain_id
        );
    }

    Ok(())
}

pub(crate) fn canonical_raw_state(status: CanonicalReconciliationStatus) -> CanonicalityState {
    match status {
        CanonicalReconciliationStatus::AwaitingAncestor => CanonicalityState::Observed,
        CanonicalReconciliationStatus::Initialized
        | CanonicalReconciliationStatus::Unchanged
        | CanonicalReconciliationStatus::Appended
        | CanonicalReconciliationStatus::GapBackfilled
        | CanonicalReconciliationStatus::ReorgReconciled => CanonicalityState::Canonical,
    }
}

pub(crate) fn insert_raw_block_candidate(
    blocks: &mut std::collections::BTreeMap<String, bigname_storage::RawBlock>,
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) {
    let candidate = provider_block_to_raw_block(chain, block, canonicality_state);
    blocks
        .entry(candidate.block_hash.clone())
        .and_modify(|existing| {
            existing.canonicality_state =
                preferred_canonicality(existing.canonicality_state, candidate.canonicality_state);
        })
        .or_insert(candidate);
}

pub(crate) fn preferred_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    if canonicality_rank(incoming) > canonicality_rank(current) {
        incoming
    } else {
        current
    }
}

pub(crate) fn canonicality_rank(state: CanonicalityState) -> u8 {
    match state {
        CanonicalityState::Observed => 0,
        CanonicalityState::Canonical => 1,
        CanonicalityState::Safe => 2,
        CanonicalityState::Finalized => 3,
        CanonicalityState::Orphaned => 4,
    }
}

pub(crate) fn provider_transaction_to_raw_transaction(
    chain: &str,
    raw_block: &RawBlock,
    transaction: &ProviderTransaction,
) -> Result<RawTransaction> {
    ensure_block_scoped_identity(
        "transaction",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &transaction.block_hash,
        transaction.block_number,
    )?;

    Ok(RawTransaction {
        chain_id: chain.to_owned(),
        block_hash: transaction.block_hash.clone(),
        block_number: transaction.block_number,
        transaction_hash: transaction.transaction_hash.clone(),
        transaction_index: transaction.transaction_index,
        from_address: transaction.from.clone(),
        to_address: transaction.to.clone(),
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn provider_receipt_to_raw_receipt(
    chain: &str,
    raw_block: &RawBlock,
    receipt: &ProviderReceipt,
) -> Result<RawReceipt> {
    ensure_block_scoped_identity(
        "receipt",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &receipt.block_hash,
        receipt.block_number,
    )?;

    Ok(RawReceipt {
        chain_id: chain.to_owned(),
        block_hash: receipt.block_hash.clone(),
        block_number: receipt.block_number,
        transaction_hash: receipt.transaction_hash.clone(),
        transaction_index: receipt.transaction_index,
        contract_address: receipt.contract_address.clone(),
        status: parse_receipt_status(receipt.status)?,
        gas_used: receipt.gas_used,
        cumulative_gas_used: receipt.cumulative_gas_used,
        logs_bloom: receipt.logs_bloom.clone(),
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn provider_log_to_raw_log(
    chain: &str,
    raw_block: &RawBlock,
    log: &ProviderLog,
) -> Result<RawLog> {
    ensure_block_scoped_identity(
        "log",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &log.block_hash,
        log.block_number,
    )?;

    Ok(RawLog {
        chain_id: chain.to_owned(),
        block_hash: log.block_hash.clone(),
        block_number: log.block_number,
        transaction_hash: log.transaction_hash.clone(),
        transaction_index: log.transaction_index,
        log_index: log.log_index,
        emitting_address: log.address.clone(),
        topics: log.topics.clone(),
        data: parse_hex_bytes(&log.data)?,
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn provider_code_observation_to_raw_code_hash(
    chain: &str,
    raw_block: &RawBlock,
    observation: &ProviderCodeObservation,
) -> Result<RawCodeHash> {
    let code_byte_length = i64::try_from(observation.code.len()).with_context(|| {
        format!(
            "provider code observation byte length {} does not fit in i64 for chain {} block {} contract {}",
            observation.code.len(),
            chain,
            raw_block.block_hash,
            observation.address
        )
    })?;

    Ok(RawCodeHash {
        chain_id: chain.to_owned(),
        block_hash: raw_block.block_hash.clone(),
        block_number: raw_block.block_number,
        contract_address: observation.address.clone(),
        code_hash: keccak256_hex(&observation.code),
        code_byte_length,
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn ensure_block_scoped_identity(
    fact_kind: &str,
    chain: &str,
    expected_block_hash: &str,
    expected_block_number: i64,
    actual_block_hash: &str,
    actual_block_number: i64,
) -> Result<()> {
    if actual_block_hash != expected_block_hash || actual_block_number != expected_block_number {
        bail!(
            "provider {} block scope mismatch for chain {} expected {}@{} got {}@{}",
            fact_kind,
            chain,
            expected_block_hash,
            expected_block_number,
            actual_block_hash,
            actual_block_number
        );
    }

    Ok(())
}

pub(crate) fn parse_receipt_status(status: Option<i64>) -> Result<Option<bool>> {
    match status {
        Some(0) => Ok(Some(false)),
        Some(1) => Ok(Some(true)),
        Some(other) => bail!("unsupported receipt status value {other}"),
        None => Ok(None),
    }
}

pub(crate) fn keccak256_hex(bytes: &[u8]) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    hex_string(&hasher.finalize())
}

pub(crate) fn parse_hex_bytes(value: &str) -> Result<Vec<u8>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() % 2 != 0 {
        bail!("invalid hex byte string with odd length");
    }

    let mut bytes = Vec::with_capacity(value.len() / 2);
    let chars = value.as_bytes();
    let mut index = 0;
    while index < chars.len() {
        let byte =
            std::str::from_utf8(&chars[index..index + 2]).context("invalid UTF-8 in hex string")?;
        bytes.push(
            u8::from_str_radix(byte, 16)
                .with_context(|| format!("failed to parse hex byte {byte}"))?,
        );
        index += 2;
    }

    Ok(bytes)
}

pub(crate) fn hex_string(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }

    output
}

pub(crate) fn provider_block_to_raw_block(
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) -> bigname_storage::RawBlock {
    bigname_storage::RawBlock {
        chain_id: chain.to_owned(),
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
            block.block_timestamp_unix_secs,
        )
        .expect("provider block timestamp must fit in OffsetDateTime"),
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
        canonicality_state,
    }
}
